package cn.edu.suda.ada.strajdb.compute.dtw.fastdtw;

import lombok.*;

@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class WindowElement {
    int x_idx, y_idx, cost_idx_left, cost_idx_up, cost_idx_corner;
}
