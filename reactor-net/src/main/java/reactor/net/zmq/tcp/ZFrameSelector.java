package reactor.net.zmq.tcp;

import org.zeromq.ZFrame;
import reactor.event.selector.ObjectSelector;

import java.util.Arrays;

/**
 * @author Jon Brisbin
 */
public class ZFrameSelector extends ObjectSelector<ZFrame> {
	public ZFrameSelector(ZFrame zf) {
		super(zf);
	}

	@Override
	public boolean matches(Object key) {
		if(!(key instanceof ZFrame)) {
			return super.matches(key);
		} else {
			byte[] b = ((ZFrame)key).getData();
			return Arrays.equals(getObject().getData(), b);
		}
	}
}
