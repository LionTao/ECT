package cn.edu.suda.ada.ect.types;

import lombok.*;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
@ToString
public class AssemblerMsg {
    String objID;
    long timeStamp;
    float longitude;
    float latitude;
}
