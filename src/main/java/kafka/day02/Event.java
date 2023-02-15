package kafka.day02;

import lombok.*;

@Getter
@Setter
@AllArgsConstructor
@ToString
@NoArgsConstructor
public class Event{
    private int guid;
    private String event_id;
    private long timestamp;
}