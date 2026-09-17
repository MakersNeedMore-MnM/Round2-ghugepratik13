import uuid
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Dict, Any, Tuple
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, and_, func

logger = logging.getLogger("hqms.queue")

from app.models import (
    Queue,
    QueueToken,
    QueueEvent,
    QueuePause,
    QueueStatus,
    TokenStatus,
    PriorityLevel,
    QueueEventType,
)
from app.domain.queue.state_machine import QueueStateMachine, InvalidStateTransitionError
from app.domain.queue.dispatcher import QueueDispatcher
from app.domain.queue.rejoin_policy import RejoinPolicyEngine
from app.domain.queue.eta_calculator import ETACalculator
from app.websockets.publisher import event_publisher



def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class QueueDomainService:
    """
    Transactional domain service executing atomic queue operations.
    """

    def __init__(self, db: AsyncSession):
        self.db = db

    async def create_token(
        self,
        queue_id: uuid.UUID,
        visit_id: uuid.UUID,
        patient_id: uuid.UUID,
        priority: PriorityLevel = PriorityLevel.NORMAL,
        initial_status: TokenStatus = TokenStatus.READY,
        actor_user_id: Optional[uuid.UUID] = None,
    ) -> QueueToken:
        """
        Atomically allocate the next token sequence for a queue, persist token, and record event.
        """
        # Fetch queue with lock
        queue = await self.db.scalar(
            select(Queue).where(Queue.id == queue_id).with_for_update()
        )
        if not queue:
            raise ValueError(f"Queue {queue_id} not found")

        queue.current_sequence += 1
        sequence_num = queue.current_sequence
        display_number = f"{queue.prefix}-{sequence_num:03d}"

        # Fetch active tokens to determine operational position
        active_tokens = await self._get_active_tokens(queue_id)
        max_pos = max([t.operational_position or 0 for t in active_tokens], default=0)
        operational_position = max_pos + 1

        token = QueueToken(
            queue_id=queue.id,
            visit_id=visit_id,
            patient_id=patient_id,
            token_display_number=display_number,
            sequence_number=sequence_num,
            priority=priority,
            status=initial_status,
            operational_position=operational_position,
            ready_at=utc_now() if initial_status == TokenStatus.READY else None,
        )
        self.db.add(token)
        await self.db.flush()

        # Audit Event
        event = QueueEvent(
            queue_id=queue.id,
            token_id=token.id,
            actor_user_id=actor_user_id,
            event_type=QueueEventType.TOKEN_CREATED,
            to_status=initial_status,
            event_data={
                "display_number": display_number,
                "priority": priority.value,
                "sequence": sequence_num,
            },
        )
        self.db.add(event)

        # Recalculate ETAs
        await self.recalculate_queue_metrics(queue_id)

        # Dispatch Token Created SMS to patient
        await self._send_patient_sms(token, "TOKEN_CREATED")
        return token

    async def call_next(
        self,
        queue_id: uuid.UUID,
        actor_user_id: Optional[uuid.UUID] = None,
        auto_complete_current: bool = True,
    ) -> Optional[QueueToken]:
        """
        Atomically complete current serving token (if requested) and call the next eligible patient.
        """
        # Lock queue
        queue = await self.db.scalar(
            select(Queue).where(Queue.id == queue_id).with_for_update()
        )
        if not queue:
            raise ValueError(f"Queue {queue_id} not found")

        # If queue is paused, automatically resume it upon calling a patient
        if queue.status == QueueStatus.PAUSED:
            await self.resume_queue(queue_id=queue_id, actor_user_id=actor_user_id)

        # Check for currently CALLED or SERVING tokens
        current_active_res = await self.db.scalars(
            select(QueueToken)
            .where(
                and_(
                    QueueToken.queue_id == queue_id,
                    QueueToken.status.in_([TokenStatus.CALLED, TokenStatus.SERVING]),
                )
            )
            .with_for_update()
        )
        current_active = list(current_active_res.all())
        for curr in current_active:
            if auto_complete_current:
                from_st = curr.status
                QueueStateMachine.validate_transition(from_st, TokenStatus.COMPLETED)
                curr.status = TokenStatus.COMPLETED
                curr.completed_at = utc_now()
                self._record_event(
                    queue_id=queue_id,
                    token_id=curr.id,
                    actor_user_id=actor_user_id,
                    event_type=QueueEventType.TOKEN_COMPLETED,
                    from_status=from_st,
                    to_status=TokenStatus.COMPLETED,
                )


        # Fetch candidate tokens for dispatching
        candidates = await self._get_active_tokens(queue_id)
        next_token = QueueDispatcher.determine_next_token(candidates)

        if next_token is None:
            return None

        # Lock and transition next token to CALLED
        QueueStateMachine.validate_transition(next_token.status, TokenStatus.CALLED)
        from_st = next_token.status
        next_token.status = TokenStatus.CALLED
        next_token.called_at = utc_now()

        self._record_event(
            queue_id=queue_id,
            token_id=next_token.id,
            actor_user_id=actor_user_id,
            event_type=QueueEventType.TOKEN_CALLED,
            from_status=from_st,
            to_status=TokenStatus.CALLED,
        )

        await self.recalculate_queue_metrics(queue_id)

        # Dispatch Token Called SMS to patient
        await self._send_patient_sms(next_token, "TOKEN_CALLED")
        return next_token

    async def mark_serving(
        self,
        token_id: uuid.UUID,
        actor_user_id: Optional[uuid.UUID] = None,
    ) -> QueueToken:
        """Doctor begins consultation: transitions CALLED -> SERVING."""
        token = await self._get_token_for_update(token_id)
        
        # If queue is paused, automatically resume it upon starting consultation
        queue = await self.db.scalar(
            select(Queue).where(Queue.id == token.queue_id).with_for_update()
        )
        if queue and queue.status == QueueStatus.PAUSED:
            await self.resume_queue(queue_id=token.queue_id, actor_user_id=actor_user_id)

        QueueStateMachine.validate_transition(token.status, TokenStatus.SERVING)
        from_st = token.status
        token.status = TokenStatus.SERVING
        token.serving_at = utc_now()

        self._record_event(
            queue_id=token.queue_id,
            token_id=token.id,
            actor_user_id=actor_user_id,
            event_type=QueueEventType.TOKEN_SERVING,
            from_status=from_st,
            to_status=TokenStatus.SERVING,
        )
        await self.db.flush()
        return token

    async def complete_token(
        self,
        token_id: uuid.UUID,
        actor_user_id: Optional[uuid.UUID] = None,
    ) -> QueueToken:
        """Finishes consultation: transitions SERVING -> COMPLETED."""
        token = await self._get_token_for_update(token_id)
        QueueStateMachine.validate_transition(token.status, TokenStatus.COMPLETED)
        from_st = token.status
        token.status = TokenStatus.COMPLETED
        token.completed_at = utc_now()

        self._record_event(
            queue_id=token.queue_id,
            token_id=token.id,
            actor_user_id=actor_user_id,
            event_type=QueueEventType.TOKEN_COMPLETED,
            from_status=from_st,
            to_status=TokenStatus.COMPLETED,
        )
        await self.recalculate_queue_metrics(token.queue_id)
        await self.db.flush()
        return token

    async def mark_missed(
        self,
        token_id: uuid.UUID,
        actor_user_id: Optional[uuid.UUID] = None,
    ) -> QueueToken:
        """Called patient did not show up: transitions CALLED -> MISSED."""
        token = await self._get_token_for_update(token_id)
        QueueStateMachine.validate_transition(token.status, TokenStatus.MISSED)
        from_st = token.status
        token.status = TokenStatus.MISSED
        token.missed_count += 1

        self._record_event(
            queue_id=token.queue_id,
            token_id=token.id,
            actor_user_id=actor_user_id,
            event_type=QueueEventType.TOKEN_MISSED,
            from_status=from_st,
            to_status=TokenStatus.MISSED,
            metadata={"missed_count": token.missed_count},
        )
        await self.recalculate_queue_metrics(token.queue_id)
        await self.db.flush()
        return token

    async def skip_token(
        self,
        token_id: uuid.UUID,
        actor_user_id: Optional[uuid.UUID] = None,
    ) -> QueueToken:
        """Staff manually skips called patient: transitions CALLED -> SKIPPED."""
        token = await self._get_token_for_update(token_id)
        QueueStateMachine.validate_transition(token.status, TokenStatus.SKIPPED)
        from_st = token.status
        token.status = TokenStatus.SKIPPED

        self._record_event(
            queue_id=token.queue_id,
            token_id=token.id,
            actor_user_id=actor_user_id,
            event_type=QueueEventType.TOKEN_SKIPPED,
            from_status=from_st,
            to_status=TokenStatus.SKIPPED,
        )
        await self.recalculate_queue_metrics(token.queue_id)
        await self.db.flush()
        return token

    async def rejoin_queue(
        self,
        token_id: uuid.UUID,
        actor_user_id: Optional[uuid.UUID] = None,
    ) -> QueueToken:
        """
        Missed patient returns: executes hospital rejoin policy and re-inserts into queue.
        """
        token = await self._get_token_for_update(token_id)
        queue = await self.db.scalar(
            select(Queue).where(Queue.id == token.queue_id).with_for_update()
        )
        if not queue:
            raise ValueError(f"Queue {token.queue_id} not found")

        QueueStateMachine.validate_transition(token.status, TokenStatus.READY)

        # Check maximum rejoin limit
        policy = queue.rejoin_policy or {}
        max_rejoins = policy.get("max_rejoins", 2)
        if token.rejoin_count >= max_rejoins:
            raise ValueError(f"Token has reached maximum allowed rejoins ({max_rejoins})")

        # Fetch active tokens
        active_tokens = await self._get_active_tokens(token.queue_id)
        candidate_positions = sorted([
            t.operational_position for t in active_tokens
            if t.operational_position is not None and t.id != token.id
        ])

        new_pos = RejoinPolicyEngine.calculate_rejoin_position(
            policy=policy,
            active_candidate_positions=candidate_positions,
        )

        from_st = token.status
        token.status = TokenStatus.READY
        token.operational_position = new_pos
        token.rejoin_count += 1
        token.ready_at = utc_now()

        self._record_event(
            queue_id=token.queue_id,
            token_id=token.id,
            actor_user_id=actor_user_id,
            event_type=QueueEventType.TOKEN_REJOINED,
            from_status=from_st,
            to_status=TokenStatus.READY,
            metadata={
                "rejoin_count": token.rejoin_count,
                "new_operational_position": new_pos,
            },
        )
        await self.recalculate_queue_metrics(token.queue_id)
        return token

    async def mark_patient_away(
        self,
        token_id: uuid.UUID,
        actor_user_id: Optional[uuid.UUID] = None,
    ) -> QueueToken:
        """Patient steps away: READY/WAITING -> AWAY."""
        token = await self._get_token_for_update(token_id)
        QueueStateMachine.validate_transition(token.status, TokenStatus.AWAY)
        from_st = token.status
        token.status = TokenStatus.AWAY

        self._record_event(
            queue_id=token.queue_id,
            token_id=token.id,
            actor_user_id=actor_user_id,
            event_type=QueueEventType.TOKEN_AWAY,
            from_status=from_st,
            to_status=TokenStatus.AWAY,
        )
        await self.recalculate_queue_metrics(token.queue_id)
        return token

    async def mark_patient_returning(
        self,
        token_id: uuid.UUID,
        actor_user_id: Optional[uuid.UUID] = None,
    ) -> QueueToken:
        """Patient indicates they are on their way back: AWAY -> RETURNING."""
        token = await self._get_token_for_update(token_id)
        QueueStateMachine.validate_transition(token.status, TokenStatus.RETURNING)
        from_st = token.status
        token.status = TokenStatus.RETURNING

        self._record_event(
            queue_id=token.queue_id,
            token_id=token.id,
            actor_user_id=actor_user_id,
            event_type=QueueEventType.TOKEN_RETURNING,
            from_status=from_st,
            to_status=TokenStatus.RETURNING,
        )
        return token

    async def mark_patient_ready(
        self,
        token_id: uuid.UUID,
        actor_user_id: Optional[uuid.UUID] = None,
    ) -> QueueToken:
        """Patient confirms they are physically back: RETURNING/AWAY/WAITING -> READY."""
        token = await self._get_token_for_update(token_id)
        QueueStateMachine.validate_transition(token.status, TokenStatus.READY)
        from_st = token.status
        token.status = TokenStatus.READY
        token.ready_at = utc_now()

        self._record_event(
            queue_id=token.queue_id,
            token_id=token.id,
            actor_user_id=actor_user_id,
            event_type=QueueEventType.TOKEN_READY,
            from_status=from_st,
            to_status=TokenStatus.READY,
        )
        await self.recalculate_queue_metrics(token.queue_id)
        return token

    async def pause_queue(
        self,
        queue_id: uuid.UUID,
        reason: str,
        expected_resume_minutes: Optional[int] = None,
        actor_user_id: Optional[uuid.UUID] = None,
    ) -> QueuePause:
        """Pause queue operations with reason and expected resumption time."""
        queue = await self.db.scalar(
            select(Queue).where(Queue.id == queue_id).with_for_update()
        )
        if not queue:
            raise ValueError(f"Queue {queue_id} not found")

        queue.status = QueueStatus.PAUSED
        now = utc_now()
        expected_resume_at = now + timedelta(minutes=expected_resume_minutes) if expected_resume_minutes else None

        pause = QueuePause(
            queue_id=queue.id,
            paused_by_user_id=actor_user_id,
            reason=reason,
            paused_at=now,
            expected_resume_at=expected_resume_at,
        )
        self.db.add(pause)

        self._record_event(
            queue_id=queue.id,
            token_id=None,
            actor_user_id=actor_user_id,
            event_type=QueueEventType.QUEUE_PAUSED,
            metadata={"reason": reason, "expected_resume_minutes": expected_resume_minutes},
        )
        await self.recalculate_queue_metrics(queue_id)
        return pause

    async def resume_queue(
        self,
        queue_id: uuid.UUID,
        actor_user_id: Optional[uuid.UUID] = None,
    ) -> Queue:
        """Resume active queue operations."""
        queue = await self.db.scalar(
            select(Queue).where(Queue.id == queue_id).with_for_update()
        )
        if not queue:
            raise ValueError(f"Queue {queue_id} not found")

        queue.status = QueueStatus.OPEN

        # Close open pause record
        open_pause = await self.db.scalar(
            select(QueuePause)
            .where(
                and_(
                    QueuePause.queue_id == queue_id,
                    QueuePause.resumed_at.is_(None),
                )
            )
            .order_by(QueuePause.paused_at.desc())
        )
        if open_pause:
            open_pause.resumed_at = utc_now()

        self._record_event(
            queue_id=queue.id,
            token_id=None,
            actor_user_id=actor_user_id,
            event_type=QueueEventType.QUEUE_RESUMED,
        )
        await self.recalculate_queue_metrics(queue_id)
        return queue

    async def recalculate_queue_metrics(self, queue_id: uuid.UUID) -> None:
        """
        Recalculates sequential operational positions and statistical ETA ranges for all active queue tokens.
        """
        queue = await self.db.scalar(select(Queue).where(Queue.id == queue_id))
        if not queue:
            return

        is_paused = queue.status == QueueStatus.PAUSED
        pause_remaining = 0
        if is_paused:
            open_pause = await self.db.scalar(
                select(QueuePause)
                .where(
                    and_(
                        QueuePause.queue_id == queue_id,
                        QueuePause.resumed_at.is_(None),
                    )
                )
                .order_by(QueuePause.paused_at.desc())
            )
            if open_pause and open_pause.expected_resume_at:
                resume_at = open_pause.expected_resume_at
                if resume_at.tzinfo is None:
                    resume_at = resume_at.replace(tzinfo=timezone.utc)
                now = utc_now()
                diff = (resume_at - now).total_seconds() / 60.0
                pause_remaining = max(1, int(diff))


        # Fetch active tokens
        active_tokens = await self._get_active_tokens(queue_id)
        sorted_tokens = QueueDispatcher.sort_tokens_by_eligibility(active_tokens)

        # Update operational positions and ETA ranges
        for index, token in enumerate(sorted_tokens):
            token.operational_position = index + 1
            min_w, max_w, _ = ETACalculator.calculate_wait_range(
                patients_ahead=index,
                avg_consult_min=queue.default_consult_time_min,
                is_paused=is_paused,
                pause_remaining_min=pause_remaining,
            )
            token.estimated_wait_min = min_w
            token.estimated_wait_max = max_w
            token.estimated_call_time_start = utc_now() + timedelta(minutes=min_w)
            token.estimated_call_time_end = utc_now() + timedelta(minutes=max_w)

    async def _get_active_tokens(self, queue_id: uuid.UUID) -> List[QueueToken]:
        """Fetch all active non-terminal tokens in a queue."""
        result = await self.db.scalars(
            select(QueueToken)
            .where(
                and_(
                    QueueToken.queue_id == queue_id,
                    QueueToken.status.in_([
                        TokenStatus.WAITING,
                        TokenStatus.READY,
                        TokenStatus.AWAY,
                        TokenStatus.RETURNING,
                    ]),
                )
            )
            .with_for_update()
        )
        return list(result.all())

    async def _get_token_for_update(self, token_id: uuid.UUID) -> QueueToken:
        token = await self.db.scalar(
            select(QueueToken).where(QueueToken.id == token_id).with_for_update()
        )
        if not token:
            raise ValueError(f"Token {token_id} not found")
        return token

    def _record_event(
        self,
        queue_id: uuid.UUID,
        token_id: Optional[uuid.UUID],
        actor_user_id: Optional[uuid.UUID],
        event_type: QueueEventType,
        from_status: Optional[TokenStatus] = None,
        to_status: Optional[TokenStatus] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> QueueEvent:
        event = QueueEvent(
            queue_id=queue_id,
            token_id=token_id,
            actor_user_id=actor_user_id,
            event_type=event_type,
            from_status=from_status,
            to_status=to_status,
            event_data=metadata or {},
        )
        self.db.add(event)
        return event

    async def _broadcast_event(
        self,
        queue_id: uuid.UUID,
        event_type: QueueEventType,
        token: Optional[QueueToken] = None,
        extra_data: Optional[Dict[str, Any]] = None,
    ) -> None:
        public_data = {
            "event_type": event_type.value,
            "timestamp": utc_now().isoformat(),
        }
        if token:
            public_data.update({
                "token_display_number": token.token_display_number,
                "status": token.status.value,
                "operational_position": token.operational_position,
                "estimated_wait_min": token.estimated_wait_min,
                "estimated_wait_max": token.estimated_wait_max,
            })
        if extra_data:
            public_data.update(extra_data)

        await event_publisher.publish_event(
            queue_id=str(queue_id),
            event_type=event_type.value,
            public_data=public_data,
        )

    async def _send_patient_sms(self, token: Optional[QueueToken], event_type: str) -> None:
        """Asynchronously formats and sends real-time SMS notification to patient."""
        if not token or not token.patient_id:
            return

        try:
            from app.domain.notifications.engine import NotificationEngine
            from app.models.patient import Patient
            from app.models.hospital import Department, Room
            from app.models.user import StaffUser
            from app.core.config import settings

            patient = await self.db.scalar(select(Patient).where(Patient.id == token.patient_id))
            if not patient or not patient.phone_number:
                return

            queue = await self.db.scalar(select(Queue).where(Queue.id == token.queue_id))
            dept_name = "General OPD"
            room_name = "Doctor Cabin"
            doctor_name = "Doctor"

            if queue:
                if queue.department_id:
                    dept = await self.db.scalar(select(Department).where(Department.id == queue.department_id))
                    if dept:
                        dept_name = dept.name
                if queue.room_id:
                    room = await self.db.scalar(select(Room).where(Room.id == queue.room_id))
                    if room:
                        room_name = f"Room {room.room_number}" if room.room_number else room.name
                if queue.doctor_user_id:
                    doc = await self.db.scalar(select(StaffUser).where(StaffUser.id == queue.doctor_user_id))
                    if doc:
                        doctor_name = doc.full_name

            live_url = f"{settings.FRONTEND_URL}/q/{token.public_tracking_id}"
            context = {
                "token_display_number": token.token_display_number,
                "department_name": dept_name,
                "room_number": room_name,
                "doctor_name": doctor_name,
                "live_url": live_url,
                "patients_ahead": max(0, (token.operational_position or 1) - 1),
            }

            engine = NotificationEngine()
            await engine.dispatch_event_notification(
                phone_number=patient.phone_number,
                event_type=event_type,
                context=context,
            )
        except Exception as e:
            logger.error(f"[SMS Dispatch Error] Failed to send {event_type} SMS for token {token.id}: {e}")

