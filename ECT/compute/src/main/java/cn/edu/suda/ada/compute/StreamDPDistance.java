package cn.edu.suda.ada.compute;

import org.jetbrains.annotations.NotNull;
import org.nd4j.linalg.api.buffer.DataType;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;

public abstract class StreamDPDistance {
    INDArray a;
    INDArray b;

    INDArray distMatrix;
    INDArray C;
    INDArray P;
    INDArray path;
    double distance;

    StreamDPDistance(INDArray a, INDArray b) {
        this.a = a;
        this.b = b;
        this.distMatrix = cdist(a, b);
    }

    static INDArray cdist(@NotNull INDArray a, @NotNull INDArray b) {
        INDArray res = Nd4j.zeros(DataType.DOUBLE, a.shape()[0] + 1, b.shape()[0] + 1);
        for (int i = 0; i < a.shape()[0]; i++) {
            for (int j = 0; j < b.shape()[0]; j++) {
                res.putScalar(i + 1, j + 1, a.getDouble(i) - b.getDouble(j));
            }
        }
        return res;
    }

    /**
     * add or remove element from window
     *
     * @return new distance
     */
    public double move() {
        return 0.0;
    }

    /**
     * remove one or more element from then end of the window
     * specifically, this method is responsible for dependency tree hit and corresponding value conduct
     *
     * @return new distance
     */
    abstract double remove();

    /**
     * add one or more element to the head of the window
     *
     * @return new distance
     */
    double add(INDArray a, INDArray b) {
        INDArray distMatrix = Nd4j.create(DataType.DOUBLE, a.shape()[0] + 1, b.shape()[0] + 1);

        return 0.0;
    }

    /**
     * calculate one specific element of accumulate distance matrix
     *
     * @return new value of that element
     */
    abstract double calcOne();
}
