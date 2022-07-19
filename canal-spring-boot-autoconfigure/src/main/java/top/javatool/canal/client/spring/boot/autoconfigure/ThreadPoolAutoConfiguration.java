package top.javatool.canal.client.spring.boot.autoconfigure;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import top.javatool.canal.client.handler.CanalThreadUncaughtExceptionHandler;
import top.javatool.canal.client.spring.boot.properties.CanalProperties;

import java.util.concurrent.*;

@Configuration
@ConditionalOnProperty(value = CanalProperties.CANAL_ASYNC, havingValue = "true", matchIfMissing = true)
public class ThreadPoolAutoConfiguration {
    /**
     * 获取cpu个数
     */
    private static final int CPU_COUNT = Runtime.getRuntime().availableProcessors();
    /**
     * 核心线程数量大小，最小两个，最大四个
     */
    private static final int CORE_POOL_SIZE = Math.max(2, Math.min(CPU_COUNT + 1, 4));
    /**
     * 线程池最大容纳线程数
     */
    private static final int MAXIMUM_POOL_SIZE = CPU_COUNT * 2 + 1;
    /**
     * 阻塞队列大小
     */
    private static final int QUEUE_CAPACITY = MAXIMUM_POOL_SIZE * 20000;
    /**
     * 线程空闲后的存活时长60S
     */
    private static final int KEEPALIVE_TIME = 60;

    /**
     * 线程池配置
     * <p>
     * ThreadPoolExecutor会根据corePoolSize和maximumPoolSize设置的界限来自动调整池的大小。
     * <p>
     * 那么我们如何配置核心线程数和最大线程数呢？
     * <p>
     * 首先我们通过Runtime.getRuntime().availableProcessors();获得当前机器的CPU核数（很多时候开发就直接把这个CPU核数作为核心线程数了）。
     * <p>
     * 接着判断一下线程池处理的程序是CPU密集型还是IO密集型。如何判断呢？CPU密集型：计算密集，CPU有许多运算要处理，需要读写IO(硬盘/内存)，IO却可以在很短的时间就完成，CPU Loading很高。IO密集型：和CPU密集型相反，系统的CPU性能相对硬盘、内存要好很多，大部分的状况是CPU在等IO (硬盘/内存) 的读写操作，CPU Loading不高。
     * <p>
     * 理论上来说：
     * 如果是CPU密集型：核心线程数 = CPU核数 + 1；
     * 而如果是IO密集型：核心线程数 = CPU核数 * 2。
     * 而具体如何配置，还是需要通过压力测试在上述理论值范围浮动确定；不同的机器环境都会导致实际数值不同。
     * <p>
     * 注：corePoolSize和maximumPoolSize的默认值是1和Integer.MAX_VALUE。
     *
     * @return {@link ExecutorService}
     */
    @Bean(destroyMethod = "shutdown")
    public ExecutorService executorService() {
        BasicThreadFactory factory = new BasicThreadFactory.Builder()
                .namingPattern("canal-execute-thread-%d")
                .uncaughtExceptionHandler(new CanalThreadUncaughtExceptionHandler()).build();
        BlockingQueue<Runnable> blockingQueue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
        ThreadPoolExecutor.CallerRunsPolicy callerRunsPolicy = new ThreadPoolExecutor.CallerRunsPolicy();
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(CORE_POOL_SIZE, MAXIMUM_POOL_SIZE, KEEPALIVE_TIME, TimeUnit.SECONDS, blockingQueue, factory, callerRunsPolicy);
        return threadPoolExecutor;
    }
}
