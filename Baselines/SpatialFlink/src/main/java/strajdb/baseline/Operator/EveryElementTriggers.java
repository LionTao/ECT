package strajdb.baseline.Operator;

import GeoFlink.spatialObjects.Point;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EveryElementTriggers extends Trigger<Object, TimeWindow> {
    private static final Logger LOG = LoggerFactory.getLogger(EveryElementTriggers.class);

    @Override
    public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.FIRE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

    }
}
