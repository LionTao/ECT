package cn.edu.suda.ada.strajdb.compute.dtw.fastdtw;

import lombok.*;

@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class LowHigh {
    int low;
    int high;
}
