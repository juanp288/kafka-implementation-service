import { IsInt, IsString, Min } from 'class-validator';

export class CreateOrderDto {
  @IsString()
  eventName: string;

  @IsInt()
  @Min(1)
  seatCount: number;
}
