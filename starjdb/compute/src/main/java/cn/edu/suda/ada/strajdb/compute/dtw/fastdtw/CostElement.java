package cn.edu.suda.ada.strajdb.compute.dtw.fastdtw;

import lombok.*;

@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class CostElement {
    double cost;
    int prev_idx;
}
