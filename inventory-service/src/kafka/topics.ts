export const TOPICS = {
  RESERVE_SEATS: 'RESERVE_SEATS',
  SEATS_RESERVED: 'SEATS_RESERVED',
  RELEASE_SEATS: 'RELEASE_SEATS',
  // Rechazo de negocio: no hay suficientes asientos. A diferencia de un fallo
  // técnico, esto se sabe al instante — avisamos a order-service para que
  // compense de inmediato en lugar de esperar el timeout de la SAGA.
  RESERVE_SEATS_REJECTED: 'RESERVE_SEATS_REJECTED',
} as const;

// Topic de Dead Letter Queue de un topic dado, p.ej. RESERVE_SEATS → RESERVE_SEATS.DLQ
export const dlqTopic = (topic: string) => `${topic}.DLQ`;
