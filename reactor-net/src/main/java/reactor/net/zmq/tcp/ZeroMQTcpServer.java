package reactor.net.zmq.tcp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZFrame;
import org.zeromq.ZLoop;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import reactor.core.Environment;
import reactor.core.Reactor;
import reactor.core.composable.Deferred;
import reactor.core.composable.Promise;
import reactor.core.composable.spec.Promises;
import reactor.function.Consumer;
import reactor.io.Buffer;
import reactor.io.encoding.Codec;
import reactor.net.NetChannel;
import reactor.net.config.ServerSocketOptions;
import reactor.net.config.SslOptions;
import reactor.net.tcp.TcpServer;
import reactor.support.NamedDaemonThreadFactory;
import reactor.util.Assert;
import reactor.util.UUIDUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author Jon Brisbin
 */
public class ZeroMQTcpServer<IN, OUT> extends TcpServer<IN, OUT> {

	private final Logger log            = LoggerFactory.getLogger(getClass());
	private final Object defaultChannel = new Object();

	private final int                       ioThreadCount;
	private final ZeroMQServerSocketOptions zeromqOpts;
	private final ExecutorService           threadPool;

	private volatile ZeroMQWorker worker;
	private volatile Future<?>    workerFuture;
	private volatile ZMQ.Socket   frontend;

	public ZeroMQTcpServer(@Nonnull Environment env,
	                       @Nonnull Reactor reactor,
	                       @Nullable InetSocketAddress listenAddress,
	                       ServerSocketOptions options,
	                       SslOptions sslOptions,
	                       @Nullable Codec<Buffer, IN, OUT> codec,
	                       @Nonnull Collection<Consumer<NetChannel<IN, OUT>>> consumers) {
		super(env, reactor, listenAddress, options, sslOptions, codec, consumers);

		this.ioThreadCount = env.getProperty("reactor.zmq.ioThreadCount", Integer.class, 1);

		if(options instanceof ZeroMQServerSocketOptions) {
			this.zeromqOpts = (ZeroMQServerSocketOptions)options;
		} else {
			this.zeromqOpts = null;
		}

		this.threadPool = Executors.newCachedThreadPool(new NamedDaemonThreadFactory("zmq-tcp"));
	}

	@Override
	public TcpServer<IN, OUT> start(@Nullable final Runnable started) {
		Assert.isNull(frontend, "This ZeroMQ server has already been started");
		this.worker = new ZeroMQWorker(started);
		this.workerFuture = threadPool.submit(this.worker);
		return this;
	}

	@Override
	protected <C> NetChannel<IN, OUT> createChannel(C ioChannel) {
		return new ZeroMQNetChannel<IN, OUT>(
				getEnvironment(),
				getReactor(),
				getReactor().getDispatcher(),
				getCodec()
		);
	}

	@Override
	public Promise<Void> shutdown() {
		if(null == worker) {
			return Promises.<Void>error(new IllegalStateException("This ZeroMQ server has not been started")).get();
		}

		Deferred<Void, Promise<Void>> d = Promises.defer(getEnvironment(), getReactor().getDispatcher());

		worker.close();
		if(!workerFuture.isDone()) {
			workerFuture.cancel(true);
		}
		threadPool.shutdownNow();
		try {
			threadPool.awaitTermination(30, TimeUnit.SECONDS);
			d.accept((Void)null);
		} catch(InterruptedException e) {
			d.accept(e);
		}
		notifyShutdown();

		return d.compose();
	}

	private class ZeroMQWorker implements Runnable {
		final UUID  id   = UUIDUtils.random();
		final ZLoop loop = new ZLoop();
		final    Runnable    started;
		volatile ZMQ.Context zmq;

		private ZeroMQWorker(Runnable started) {this.started = started;}

		@Override
		public void run() {
			if(null != zeromqOpts && null != zeromqOpts.context()) {
				zmq = zeromqOpts.context();
			} else {
				zmq = ZMQ.context(ioThreadCount);
			}
			final int socketType = (null != zeromqOpts ? zeromqOpts.socketType() : ZMQ.ROUTER);
			frontend = zmq.socket(socketType);
			frontend.setIdentity(id.toString().getBytes());
			frontend.setReceiveBufferSize(getOptions().rcvbuf());
			frontend.setSendBufferSize(getOptions().sndbuf());
			frontend.setBacklog(getOptions().backlog());
			if(getOptions().keepAlive()) {
				frontend.setTCPKeepAlive(1);
			}

			ZLoop.IZLoopHandler handler = new ZLoop.IZLoopHandler() {
				@Override
				public int handle(ZLoop loop, ZMQ.PollItem item, Object arg) {
					ZMsg msg = ZMsg.recvMsg(frontend);

					Object key;
					switch(socketType) {
						case ZMQ.ROUTER:
							key = msg.popString();
							break;
						default:
							key = defaultChannel;
					}
					ZeroMQNetChannel<IN, OUT> netChannel = (ZeroMQNetChannel<IN, OUT>)select(key);
					netChannel.setSocket(frontend);

					ZFrame content;
					while(null != (content = msg.pop())) {
						netChannel.read(Buffer.wrap(content.getData()));
					}
					msg.destroy();

					return 0;
				}
			};
			ZMQ.PollItem pollInput = new ZMQ.PollItem(frontend, ZMQ.Poller.POLLIN);
			loop.addPoller(pollInput, handler, null);

			if(log.isInfoEnabled()) {
				log.info("BIND: starting ZeroMQ server on {}", getListenAddress());
			}
			String addr = String.format("tcp://%s:%s",
			                            getListenAddress().getHostString(),
			                            getListenAddress().getPort());
			frontend.bind(addr);

			notifyStart(started);

			loop.start();
		}

		void close() {
			loop.destroy();
			frontend.close();
			zmq.close();
		}
	}

}
