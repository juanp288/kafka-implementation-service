export const TOPICS = {
  RESERVE_SEATS: 'RESERVE_SEATS',
  SEATS_RESERVED: 'SEATS_RESERVED',
  RELEASE_SEATS: 'RELEASE_SEATS',
} as const;

// Topic de Dead Letter Queue de un topic dado, p.ej. RESERVE_SEATS → RESERVE_SEATS.DLQ
export const dlqTopic = (topic: string) => `${topic}.DLQ`;
