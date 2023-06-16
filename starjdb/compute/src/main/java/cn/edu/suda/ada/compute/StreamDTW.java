package cn.edu.suda.ada.compute;

public class StreamDTW extends StreamDPDistance{
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
