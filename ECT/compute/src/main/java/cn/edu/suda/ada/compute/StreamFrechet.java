package cn.edu.suda.ada.compute;

import org.nd4j.linalg.api.ndarray.INDArray;

public class StreamFrechet extends StreamDPDistance {
    StreamFrechet(INDArray a, INDArray b) {
        super(a, b);
    }

    /**
     * Frechet has multiple possible path thus need to loop
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
