package cn.edu.suda.ada.compute;

import org.nd4j.linalg.api.ndarray.INDArray;

public class StreamDTW extends StreamDPDistance{
    StreamDTW(INDArray a, INDArray b) {
        super(a, b);
    }

    /**
     * DTW only has one path which can be figure out easily
     */
    @Override
    double remove() {
        return 0;
    }

    @Override
    double calcOne() {
        return 0;
    }
}
