import {
  PatientLiveTokenView,
  QueueSummary,
  QueueToken,
  Queue,
  Hospital,
  StaffUser,
  PriorityLevel,
  Gender,
  UserRole,
  QueueStatus,
} from "@/types/queue";

export type { UserRole, QueueStatus, PriorityLevel, Gender, StaffUser, Hospital, Queue, QueueToken };


const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api/v1";

class ApiError extends Error {
  status: number;
  data: any;

  constructor(status: number, message: string, data?: any) {
    super(message);
    this.status = status;
    this.data = data;
  }
}

async function request<T>(endpoint: string, options: RequestInit = {}): Promise<T> {
  const token = typeof window !== "undefined" ? localStorage.getItem("hqms_staff_token") : null;

  const headers: HeadersInit = {
    "Content-Type": "application/json",
    ...(token ? { Authorization: `Bearer ${token}` } : {}),
    ...options.headers,
  };

  const url = `${API_BASE}${endpoint}`;
  const response = await fetch(url, {
    ...options,
    headers,
  });

  if (!response.ok) {
    let errorData = null;
    try {
      errorData = await response.json();
    } catch {
      // Ignored
    }
    const message = errorData?.detail || `HTTP Error ${response.status}: ${response.statusText}`;

    // Auto-clear stale/invalid tokens on 401 to prevent permanent lockout
    if (response.status === 401 && typeof window !== "undefined") {
      const isAuthPage = window.location.pathname.includes("/login");
      if (!isAuthPage) {
        localStorage.removeItem("hqms_staff_token");
        localStorage.removeItem("hqms_user_role");
        localStorage.removeItem("hqms_user");
        const redirectUrl = window.location.pathname.includes("/admin/hospitals")
          ? "/platform-control/login"
          : "/login";
        window.location.href = redirectUrl;
      }
    }

    throw new ApiError(response.status, message, errorData);
  }

  if (response.status === 204) {
    return {} as T;
  }

  return response.json();
}

export const api = {
  auth: {
    login: async (email: string, password: string) => {
      const data = await request<{ access_token: string; user: StaffUser }>("/auth/login/json", {
        method: "POST",
        body: JSON.stringify({ email, password }),
      });
      if (typeof window !== "undefined" && data.access_token) {
        localStorage.setItem("hqms_staff_token", data.access_token);
        localStorage.setItem("hqms_user_role", data.user.role);
        localStorage.setItem("hqms_user", JSON.stringify(data.user));
      }
      return data;
    },
    registerHospital: async (payload: {
      hospital_name: string;
      admin_name: string;
      admin_email: string;
      admin_password: string;
      phone_number?: string;
      address?: string;
      tagline?: string;
    }) => {
      const data = await request<{ access_token: string; user: StaffUser }>("/auth/register-hospital", {
        method: "POST",
        body: JSON.stringify(payload),
      });
      if (typeof window !== "undefined" && data.access_token) {
        localStorage.setItem("hqms_staff_token", data.access_token);
        localStorage.setItem("hqms_user_role", data.user.role);
        localStorage.setItem("hqms_user", JSON.stringify(data.user));
      }
      return data;
    },
    logout: () => {
      if (typeof window !== "undefined") {
        localStorage.removeItem("hqms_staff_token");
        localStorage.removeItem("hqms_user_role");
        localStorage.removeItem("hqms_user");
      }
    },
    me: () => request<StaffUser>("/auth/me"),
    getUser: (): StaffUser | null => {
      if (typeof window === "undefined") return null;
      const u = localStorage.getItem("hqms_user");
      return u ? JSON.parse(u) : null;
    },
  },


  hospitals: {
    list: () => request<Hospital[]>("/hospitals/"),
  },

  queues: {
    list: (deptId?: string) =>
      request<Queue[]>(deptId ? `/queues/?department_id=${deptId}` : "/queues/"),
    get: (queueId: string) => request<Queue>(`/queues/${queueId}`),
  },

  reception: {
    issueWalkIn: (data: {
      queue_id: string;
      patient_name: string;
      patient_phone?: string;
      patient_gender?: Gender;
      priority?: PriorityLevel;
      notes?: string;
    }) =>
      request<QueueToken>("/reception/tokens/walk-in", {
        method: "POST",
        body: JSON.stringify(data),
      }),
    getSummary: (queueId: string) => request<QueueSummary>(`/reception/queues/${queueId}/summary`),
    searchPatients: (query: string) =>
      request<any[]>(`/reception/patients/search?query=${encodeURIComponent(query)}`),
  },

  doctor: {
    callNext: (queueId: string, autoCompleteCurrent: boolean = true) =>
      request<QueueToken | null>(`/doctor/queues/${queueId}/call-next?auto_complete_current=${autoCompleteCurrent}`, {
        method: "POST",
      }),
    startServing: (tokenId: string) =>
      request<QueueToken>(`/doctor/tokens/${tokenId}/start-serving`, { method: "POST" }),
    complete: (tokenId: string) =>
      request<QueueToken>(`/doctor/tokens/${tokenId}/complete`, { method: "POST" }),
    skip: (tokenId: string) =>
      request<QueueToken>(`/doctor/tokens/${tokenId}/skip`, { method: "POST" }),
    missed: (tokenId: string) =>
      request<QueueToken>(`/doctor/tokens/${tokenId}/missed`, { method: "POST" }),
    rejoin: (tokenId: string) =>
      request<QueueToken>(`/doctor/tokens/${tokenId}/rejoin`, { method: "POST" }),
    pause: (queueId: string, reason: string, expectedResumeMinutes?: number) =>
      request<Queue>(`/doctor/queues/${queueId}/pause`, {
        method: "POST",
        body: JSON.stringify({ reason, expected_resume_minutes: expectedResumeMinutes }),
      }),
    resume: (queueId: string) =>
      request<Queue>(`/doctor/queues/${queueId}/resume`, { method: "POST" }),
  },

  patient: {
    getToken: (publicId: string) => request<PatientLiveTokenView>(`/patient/tokens/${publicId}`),
    markAway: (publicId: string) =>
      request<PatientLiveTokenView>(`/patient/tokens/${publicId}/away`, { method: "POST" }),
    markReturning: (publicId: string) =>
      request<PatientLiveTokenView>(`/patient/tokens/${publicId}/returning`, { method: "POST" }),
    markReady: (publicId: string) =>
      request<PatientLiveTokenView>(`/patient/tokens/${publicId}/ready`, { method: "POST" }),
  },

  platform: {
    listHospitals: () => request<HospitalSummary[]>("/platform/hospitals"),
    provisionHospital: (data: HospitalProvisionRequest) =>
      request<HospitalProvisionResponse>("/platform/hospitals", {
        method: "POST",
        body: JSON.stringify(data),
      }),
    resendWelcomeEmail: (hospitalId: string) =>
      request<{ status: string; sent: boolean; recipient: string }>(
        `/platform/hospitals/${hospitalId}/send-welcome-email`,
        { method: "POST" }
      ),
    updateHospital: (hospitalId: string, data: { name?: string; slug?: string; address?: string; phone?: string; is_active?: boolean }) =>
      request<HospitalSummary>(`/platform/hospitals/${hospitalId}`, {
        method: "PATCH",
        body: JSON.stringify(data),
      }),
    updateStatus: (hospitalId: string, isActive: boolean) =>
      request<HospitalSummary>(`/platform/hospitals/${hospitalId}`, {
        method: "PATCH",
        body: JSON.stringify({ is_active: isActive }),
      }),
    deleteHospital: (hospitalId: string) =>
      request<{ status: string; message: string; deleted_hospital_id: string }>(
        `/platform/hospitals/${hospitalId}`,
        { method: "DELETE" }
      ),
  },

  hospitalAdmin: {
    getOverview: (hospitalId?: string) =>
      request<HospitalAdminOverview>(
        hospitalId ? `/hospital-admin/overview?hospital_id=${hospitalId}` : "/hospital-admin/overview"
      ),
    resendStaffInvite: (userId: string, hospitalId?: string) =>
      request<{ status: string; sent: boolean; recipient: string }>(
        hospitalId
          ? `/hospital-admin/staff/${userId}/resend-invite?hospital_id=${hospitalId}`
          : `/hospital-admin/staff/${userId}/resend-invite`,
        { method: "POST" }
      ),
    createDepartment: (data: { branch_id?: string; name: string; code: string }, hospitalId?: string) =>
      request<DepartmentItem>(
        hospitalId ? `/hospital-admin/departments?hospital_id=${hospitalId}` : "/hospital-admin/departments",
        {
          method: "POST",
          body: JSON.stringify(data),
        }
      ),
    updateDepartment: (deptId: string, data: { name?: string; code?: string }, hospitalId?: string) =>
      request<DepartmentItem>(
        hospitalId ? `/hospital-admin/departments/${deptId}?hospital_id=${hospitalId}` : `/hospital-admin/departments/${deptId}`,
        {
          method: "PATCH",
          body: JSON.stringify(data),
        }
      ),
    deleteDepartment: (deptId: string, hospitalId?: string) =>
      request<{ status: string; message: string }>(
        hospitalId ? `/hospital-admin/departments/${deptId}?hospital_id=${hospitalId}` : `/hospital-admin/departments/${deptId}`,
        { method: "DELETE" }
      ),
    createRoom: (data: { department_id: string; name: string; room_number: string }, hospitalId?: string) =>
      request<RoomItem>(
        hospitalId ? `/hospital-admin/rooms?hospital_id=${hospitalId}` : "/hospital-admin/rooms",
        {
          method: "POST",
          body: JSON.stringify(data),
        }
      ),
    updateRoom: (roomId: string, data: { department_id?: string; name?: string; room_number?: string }, hospitalId?: string) =>
      request<RoomItem>(
        hospitalId ? `/hospital-admin/rooms/${roomId}?hospital_id=${hospitalId}` : `/hospital-admin/rooms/${roomId}`,
        {
          method: "PATCH",
          body: JSON.stringify(data),
        }
      ),
    deleteRoom: (roomId: string, hospitalId?: string) =>
      request<{ status: string; message: string }>(
        hospitalId ? `/hospital-admin/rooms/${roomId}?hospital_id=${hospitalId}` : `/hospital-admin/rooms/${roomId}`,
        { method: "DELETE" }
      ),
    inviteStaff: (
      data: {
        branch_id?: string;
        email: string;
        full_name: string;
        password: string;
        phone_number?: string;
        role: UserRole;
      },
      hospitalId?: string
    ) =>
      request<StaffItem>(
        hospitalId ? `/hospital-admin/staff?hospital_id=${hospitalId}` : "/hospital-admin/staff",
        {
          method: "POST",
          body: JSON.stringify(data),
        }
      ),
    updateStaff: (
      userId: string,
      data: {
        full_name?: string;
        email?: string;
        phone_number?: string;
        role?: UserRole;
        is_active?: boolean;
        password?: string;
      },
      hospitalId?: string
    ) =>
      request<StaffItem>(
        hospitalId ? `/hospital-admin/staff/${userId}?hospital_id=${hospitalId}` : `/hospital-admin/staff/${userId}`,
        {
          method: "PATCH",
          body: JSON.stringify(data),
        }
      ),
    deleteStaff: (userId: string, hospitalId?: string) =>
      request<{ status: string; message: string }>(
        hospitalId ? `/hospital-admin/staff/${userId}?hospital_id=${hospitalId}` : `/hospital-admin/staff/${userId}`,
        { method: "DELETE" }
      ),
    createQueue: (
      data: {
        department_id: string;
        doctor_user_id?: string;
        room_id?: string;
        name: string;
        prefix: string;
        default_consult_time_min?: number;
      },
      hospitalId?: string
    ) =>
      request<QueueItem>(
        hospitalId ? `/hospital-admin/queues?hospital_id=${hospitalId}` : "/hospital-admin/queues",
        {
          method: "POST",
          body: JSON.stringify(data),
        }
      ),
    updateQueue: (
      queueId: string,
      data: {
        department_id?: string;
        doctor_user_id?: string;
        room_id?: string;
        name?: string;
        prefix?: string;
        default_consult_time_min?: number;
        status?: QueueStatus;
      },
      hospitalId?: string
    ) =>
      request<QueueItem>(
        hospitalId ? `/hospital-admin/queues/${queueId}?hospital_id=${hospitalId}` : `/hospital-admin/queues/${queueId}`,
        {
          method: "PATCH",
          body: JSON.stringify(data),
        }
      ),
    deleteQueue: (queueId: string, hospitalId?: string) =>
      request<{ status: string; message: string }>(
        hospitalId ? `/hospital-admin/queues/${queueId}?hospital_id=${hospitalId}` : `/hospital-admin/queues/${queueId}`,
        { method: "DELETE" }
      ),
  },
};

export interface HospitalProvisionRequest {
  name: string;
  slug?: string;
  admin_name: string;
  admin_email: string;
  admin_password: string;
  admin_phone?: string;
  branch_name?: string;
  department_name?: string;
  department_code?: string;
  address?: string;
  phone?: string;
}

export interface HospitalProvisionResponse {
  id: string;
  name: string;
  slug: string;
  is_active: boolean;
  admin_user_id: string;
  admin_email: string;
  default_branch_id: string;
  default_department_id: string;
  default_queue_id: string;
  created_at: string;
}

export interface HospitalSummary {
  id: string;
  name: string;
  slug: string;
  is_active: boolean;
  address?: string;
  phone?: string;
  branch_count: number;
  staff_count: number;
  queue_count: number;
  created_at: string;
}

export interface DepartmentItem {
  id: string;
  branch_id: string;
  name: string;
  code: string;
  room_count: number;
  queue_count: number;
}

export interface RoomItem {
  id: string;
  department_id: string;
  name: string;
  room_number: string;
}

export interface StaffItem {
  id: string;
  email: string;
  full_name: string;
  role: UserRole;
  phone_number?: string;
  is_active: boolean;
  created_at: string;
}

export interface QueueItem {
  id: string;
  department_id: string;
  department_name?: string;
  doctor_user_id?: string;
  doctor_name?: string;
  room_id?: string;
  room_number?: string;
  name: string;
  prefix: string;
  status: QueueStatus;
  default_consult_time_min: number;
  current_sequence: number;
}

export interface HospitalAdminOverview {
  hospital_id: string;
  hospital_name: string;
  hospital_slug: string;
  branches: Array<{ id: string; name: string; code: string }>;
  departments: DepartmentItem[];
  rooms: RoomItem[];
  staff: StaffItem[];
  queues: QueueItem[];
}


