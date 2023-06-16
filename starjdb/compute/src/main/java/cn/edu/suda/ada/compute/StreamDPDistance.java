package cn.edu.suda.ada.compute;

public abstract class StreamDPDistance {

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
     * @return new distance
     */
    abstract double remove();

    /**
     * add one or more element to the head of the window
     *
     * @return new distance
     */
    double add() {
        return 0.0;
    }

    /**
     * calculate one specific element of accumulate distance matrix
     *
     * @return new value of that element
     */
    abstract double calcOne();
}
