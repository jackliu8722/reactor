package reactor.net.zmq.tcp;

import org.zeromq.ZMQ;
import reactor.net.config.ServerSocketOptions;
import reactor.util.Assert;

/**
 * {@link reactor.net.config.ServerSocketOptions} that include ZeroMQ-specific configuration options.
 *
 * @author Jon Brisbin
 */
public class ZeroMQServerSocketOptions extends ServerSocketOptions {

	private ZMQ.Context context;

	/**
	 * Get the {@link org.zeromq.ZMQ.Context} to use for IO.
	 *
	 * @return the {@link org.zeromq.ZMQ.Context} to use
	 */
	public ZMQ.Context context() {
		return context;
	}

	/**
	 * Set the {@link org.zeromq.ZMQ.Context} to use for IO.
	 *
	 * @param context
	 * 		the {@link org.zeromq.ZMQ.Context} to use
	 *
	 * @return {@literal this}
	 */
	public ZeroMQServerSocketOptions context(ZMQ.Context context) {
		Assert.notNull(context, "ZeroMQ Context cannot be null");
		this.context = context;
		return this;
	}

}
