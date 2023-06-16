package cn.edu.suda.ada.compute;

public class StreamFrechet extends StreamDPDistance {
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
