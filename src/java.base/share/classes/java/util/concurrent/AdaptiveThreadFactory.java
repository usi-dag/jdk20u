package java.util.concurrent;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Optional;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Comment
 */
public class AdaptiveThreadFactory implements ThreadFactory, AutoCloseable {

  /* Private members */

  // factory managament
  private static AtomicInteger adaptiveThreadFactoryCounter;
  private static ConcurrentHashMap<Thread, AdaptiveThreadFactory> associations;

  // user specification
  private int adaptiveThreadFactoryId;
  private long parkingTimeWindowLength; // in nanoseconds
  private long threadCreationTimeWindowLength; // in nanoseconds
  private ThreadTypeSelector threadTypeSelector;
  private int cpuUsageSamplingPeriod; // in milliseconds
  private int numberRelevantCpuUsageSamples;
  private Optional<Integer> stateQueryInterval; // in milliseconds
  private Optional<Integer> numberRecurrencesUntilTransition;
  private Optional<Runnable> threadCreationHandler;

  // internal use

  private ConcurrentHashMap<Thread, Boolean> marks;
  private ExecutionMode executionMode;
  private ThreadFactory platformThreadFactory;
  private ThreadFactory virtualThreadFactory;
  private Object operatingSystemMXBeanObject;
  private Method getCpuLoadMethod;
  private Supplier<Double> systemCpuUsageSupplier;
  private Method getProcessCpuLoadMethod;
  private Supplier<Double> processCpuUsageSupplier;
  private Thread cpuUsageSampler;
  private ConcurrentLinkedQueue<Double> cpuUsageSamples;
  private ConcurrentLinkedQueue<Long> parkingTimes;
  private ConcurrentLinkedQueue<Long> threadCreationTimes;

  // ExecutionMode.HOMOGENEOUS
  private Thread transitionManager;
  private ThreadType currentThreadType;
  private LinkedList<ThreadType> queryResults;

  static {
    adaptiveThreadFactoryCounter = new AtomicInteger(0);
    associations = new ConcurrentHashMap<Thread, AdaptiveThreadFactory>();
  }

  // User specification

  private class UserSpecificationRequirement {

    private boolean condition;
    private String message;

    public UserSpecificationRequirement setCondition(boolean condition) {
      this.condition = condition;
      return this;
    }

    public UserSpecificationRequirement setMessage(String message) {
      this.message = message;
      return this;
    }

    public void check() {
      if (!this.condition) {
        throw new IllegalArgumentException(this.message);
      }
    }
  }

  private void validateUserSpecification() {
    LinkedList<UserSpecificationRequirement> requirements = new LinkedList<UserSpecificationRequirement>();
    requirements.add(
      new UserSpecificationRequirement()
        .setCondition(
          (
            this.stateQueryInterval.isPresent() &&
            this.numberRecurrencesUntilTransition.isPresent()
          ) ||
          (
            this.stateQueryInterval.isEmpty() &&
            this.numberRecurrencesUntilTransition.isEmpty()
          )
        )
        .setMessage(
          "Either all or none of the parameters {stateQueryInterval, numberRecurrencesUntilTransition} must be set."
        )
    );
    for (final UserSpecificationRequirement requirement : requirements) {
      requirement.check();
    }
  }

  // Initialisation

  private boolean homogeneousExecutionModeInEffect() {
    final boolean homogeneousExecutionModeInEffect =
      this.stateQueryInterval.isPresent() &&
      this.numberRecurrencesUntilTransition.isPresent();
    return homogeneousExecutionModeInEffect;
  }

  private void startPeriodicallyActiveThreads() {
    startCpuUsageSampler();
    if (homogeneousExecutionModeInEffect()) {
      startTransitionManager();
    }
  }

  private void initialise() {
    this.platformThreadFactory = Thread.ofPlatform().factory();
    this.virtualThreadFactory = Thread.ofVirtual().factory();
    this.marks = new ConcurrentHashMap<Thread, Boolean>();
    this.parkingTimes = new ConcurrentLinkedQueue<Long>();
    this.threadCreationTimes = new ConcurrentLinkedQueue<Long>();
    createCpuUsageSupplier();
    this.cpuUsageSamples = new ConcurrentLinkedQueue<Double>();
    if (homogeneousExecutionModeInEffect()) {
      this.executionMode = ExecutionMode.HOMOGENEOUS;
      this.queryResults = new LinkedList<ThreadType>();
      this.currentThreadType = ThreadType.PLATFORM;
    } else {
      this.executionMode = ExecutionMode.HETEROGENEOUS;
    }
    startPeriodicallyActiveThreads();
  }

  private static long getDefaultParkingTimeWindowLength() {
    return 200;
  }

  private static long getDefaultThreadCreationTimeWindowLength() {
    return 200;
  }

  private static ThreadTypeSelector getDefaultThreadTypeSelector() {
    ThreadTypeSelector defaultThreadTypeSelector = (
      long numberParkingsInTimeWindow,
      long numberThreadCreationsInTimeWindow,
      double cpuUsage,
      long numberFactoryThreads,
      Optional<AdaptiveThreadFactory.ThreadType> currentThreadType
    ) -> {
      return ThreadType.PLATFORM;
    };
    return defaultThreadTypeSelector;
  }

  private static int getDefaultCpuUsageSamplingPeriod() {
    return 100;
  }

  private static int getDefaultNumberRelevantCpuUsageSamples() {
    return 5;
  }

  private static Optional<Integer> getDefaultStateQueryInterval() {
    return Optional.of(1500);
  }

  private static Optional<Integer> getDefaultNumberRecurrencesUntilTransition() {
    return Optional.of(5);
  }

  private static Optional<Runnable> getDefaultThreadCreationHandler() {
    return Optional.empty();
  }

  // Transitioning

  private boolean shallTransition(ThreadType newQueryResult) {
    this.queryResults.add(newQueryResult);
    if (queryResults.size() > this.numberRecurrencesUntilTransition.get()) {
      queryResults.poll();
    }
    final boolean identicalQueryResults =
      this.queryResults.stream().distinct().count() == 1;
    if (identicalQueryResults) {
      if (!newQueryResult.equals(this.currentThreadType)) {
        this.currentThreadType = newQueryResult;
        return true;
      }
    }
    return false;
  }

  private void performTransition() {
    this.marks.replaceAll((Thread thread, Boolean markedForTermination) -> true
      );
  }

  private void startTransitionManager() {
    this.transitionManager =
      new Thread(() -> {
        while (!Thread.currentThread().isInterrupted()) {
          final ThreadType newQueryResult = queryMonitor();
          final boolean shallTransition = shallTransition(newQueryResult);
          if (shallTransition) {
            performTransition();
          }
          try {
            Thread.sleep(this.stateQueryInterval.get());
          } catch (InterruptedException interruptedException) {
            return;
          }
        }
      });
    this.transitionManager.setDaemon(true);
    this.transitionManager.start();
  }

  private void stopTransitionManager() {
    this.transitionManager.interrupt();
    try {
      this.transitionManager.join();
    } catch (InterruptedException interruptedException) {
      throw new RuntimeException(interruptedException.getMessage());
    }
  }

  private ThreadType queryMonitor() {
    Optional<ThreadType> currentFactoryThreadType;
    if (homogeneousExecutionModeInEffect()) {
      currentFactoryThreadType = Optional.of(this.currentThreadType);
    } else {
      currentFactoryThreadType = Optional.empty();
    }
    return this.threadTypeSelector.selectThreadType(
        getNumberParkingsInTimeWindow(),
        getNumberThreadCreationsInTimeWindow(),
        computeAverageCpuUsage(),
        getNumberThreads(),
        currentFactoryThreadType
      );
  }

  // CPU usage

  private void createCpuUsageSupplier() {
    try {
      ClassLoader systemClassLoader = ClassLoader.getSystemClassLoader();
      Class<?> operatingSystemMXBeanClass = systemClassLoader.loadClass(
        "com.sun.management.OperatingSystemMXBean"
      );
      Class<?> managementFactoryClass = systemClassLoader.loadClass(
        "java.lang.management.ManagementFactory"
      );
      Method getOperatingSystemMXBeanMethod = managementFactoryClass.getDeclaredMethod(
        "getOperatingSystemMXBean"
      );
      this.operatingSystemMXBeanObject =
        operatingSystemMXBeanClass.cast(
          getOperatingSystemMXBeanMethod.invoke(null)
        );
      this.getCpuLoadMethod =
        operatingSystemMXBeanClass.getDeclaredMethod("getCpuLoad");
      this.getProcessCpuLoadMethod =
        operatingSystemMXBeanClass.getDeclaredMethod("getProcessCpuLoad");
      this.systemCpuUsageSupplier =
        () -> {
          try {
            return (double) this.getCpuLoadMethod.invoke(
                this.operatingSystemMXBeanObject
              );
          } catch (Exception exception) {
            throw new RuntimeException(exception.getMessage());
          }
        };
      this.processCpuUsageSupplier =
        () -> {
          try {
            return (double) this.getProcessCpuLoadMethod.invoke(
                this.operatingSystemMXBeanObject
              );
          } catch (Exception exception) {
            throw new RuntimeException(exception.getMessage());
          }
        };
    } catch (Exception exception) {
      throw new RuntimeException(exception.getMessage());
    }
  }

  private void startCpuUsageSampler() {
    this.cpuUsageSampler =
      new Thread(() -> {
        while (!Thread.currentThread().isInterrupted()) {
          final double cpuUsageSample = this.systemCpuUsageSupplier.get();
          this.cpuUsageSamples.add(cpuUsageSample);
          if (
            this.cpuUsageSamples.size() > this.numberRelevantCpuUsageSamples
          ) {
            this.cpuUsageSamples.poll();
          }
          try {
            Thread.sleep(this.cpuUsageSamplingPeriod);
          } catch (InterruptedException interruptedException) {
            return;
          }
        }
      });
    this.cpuUsageSampler.setDaemon(true);
    this.cpuUsageSampler.start();
  }

  private void stopCpuUsageSampler() {
    this.cpuUsageSampler.interrupt();
    try {
      this.cpuUsageSampler.join();
    } catch (InterruptedException interruptedException) {
      throw new RuntimeException(interruptedException.getMessage());
    }
  }

  private double computeAverageCpuUsage() {
    LinkedList<Double> currentCpuUsageSamples = new LinkedList<Double>();
    Iterator<Double> iterator = this.cpuUsageSamples.iterator();
    while (iterator.hasNext()) {
      Double cpuUsageSample = iterator.next();
      currentCpuUsageSamples.add(cpuUsageSample);
    }
    final double averageCpuUsage = currentCpuUsageSamples
      .stream()
      .mapToDouble(Double::doubleValue)
      .average()
      .orElse(0.0);
    return averageCpuUsage;
  }

  // Recording

  private void addParkingTime(long parkingTime) {
    this.parkingTimes.add(parkingTime);
  }

  private void addThreadCreationTime(long threadCreationTime) {
    this.threadCreationTimes.add(threadCreationTime);
  }

  private long countRecentAndRemoveOldValues(
    ConcurrentLinkedQueue<Long> values,
    long lowerBound
  ) {
    values.removeIf((Long value) -> value < lowerBound);
    return values.size();
  }

  private void recordThreadCreation() {
    addThreadCreationTime(System.nanoTime());
  }

  // Registration

  private Runnable augmentTask(Runnable originalTask) {
    final Runnable augmentedTask = () -> {
      recordThreadCreation();
      originalTask.run();
      if (isMarkedForTransition(Thread.currentThread())) {
        this.threadCreationHandler.ifPresent((Runnable runnable) ->
            runnable.run()
          );
      }
      deregister(Thread.currentThread());
    };
    return augmentedTask;
  }

  private void register(Thread thread) {
    associations.put(thread, this);
    this.marks.put(thread, false);
  }

  private void deregister(Thread thread) {
    associations.remove(thread);
    this.marks.remove(thread);
  }

  private ConcurrentHashMap<Thread, Boolean> getMarks() {
    return this.marks;
  }

  // Types

  private enum ExecutionMode {
    HETEROGENEOUS,
    HOMOGENEOUS,
  }

  // Construction

  /**
   * Comment
   *
   * @param   parkingTimeWindowLength Comment
   * @param   threadCreationTimeWindowLength Comment
   * @param   threadTypeSelector Comment
   * @param   cpuUsageSamplingPeriod Comment
   * @param   numberRelevantCpuUsageSamples Comment
   * @param   stateQueryInterval Comment
   * @param   numberRecurrencesUntilTransition Comment
   * @param   threadCreationHandler Comment
   */
  private AdaptiveThreadFactory(
    long parkingTimeWindowLength,
    long threadCreationTimeWindowLength,
    ThreadTypeSelector threadTypeSelector,
    int cpuUsageSamplingPeriod,
    int numberRelevantCpuUsageSamples,
    Optional<Integer> stateQueryInterval,
    Optional<Integer> numberRecurrencesUntilTransition,
    Optional<Runnable> threadCreationHandler
  ) {
    this.adaptiveThreadFactoryId =
      adaptiveThreadFactoryCounter.incrementAndGet();
    this.parkingTimeWindowLength = parkingTimeWindowLength * 1_000_000;
    this.threadCreationTimeWindowLength =
      threadCreationTimeWindowLength * 1_000_000;
    this.threadTypeSelector = threadTypeSelector;
    this.cpuUsageSamplingPeriod = cpuUsageSamplingPeriod;
    this.numberRelevantCpuUsageSamples = numberRelevantCpuUsageSamples;
    this.stateQueryInterval = stateQueryInterval;
    this.numberRecurrencesUntilTransition = numberRecurrencesUntilTransition;
    this.threadCreationHandler = threadCreationHandler;
    validateUserSpecification();
    initialise();
  }

  /* Public members */

  // Construction

  /**
   * Comment
   *
   */
  public AdaptiveThreadFactory() {
    this(
      getDefaultParkingTimeWindowLength(),
      getDefaultThreadCreationTimeWindowLength(),
      getDefaultThreadTypeSelector(),
      getDefaultCpuUsageSamplingPeriod(),
      getDefaultNumberRelevantCpuUsageSamples(),
      getDefaultStateQueryInterval(),
      getDefaultNumberRecurrencesUntilTransition(),
      getDefaultThreadCreationHandler()
    );
  }

  // Thread creation

  /**
   * Comment
   * @return Comment
   */
  public Thread newThread(Runnable originalTask) {
    final Runnable augmentedTask = augmentTask(originalTask);
    Thread newThread;
    if (!homogeneousExecutionModeInEffect()) {
      final ThreadType newQueryResult = queryMonitor();
      if (newQueryResult.equals(ThreadType.PLATFORM)) {
        newThread = platformThreadFactory.newThread(augmentedTask);
      } else {
        newThread = virtualThreadFactory.newThread(augmentedTask);
      }
    } else {
      if (this.currentThreadType.equals(ThreadType.PLATFORM)) {
        newThread = platformThreadFactory.newThread(augmentedTask);
      } else {
        newThread = virtualThreadFactory.newThread(augmentedTask);
      }
    }
    register(newThread);
    return newThread;
  }

  // Closing

  /*
   * Comment
   */
  public void close() {
    stopCpuUsageSampler();
    if (homogeneousExecutionModeInEffect()) {
      stopTransitionManager();
    }
  }

  // Recording

  /**
   * Comment
   *
   * @param thread Comment
   */
  public static void recordParkingIfExistsAssociation(Thread thread) {
    AdaptiveThreadFactory adaptiveThreadFactory = associations.get(thread);
    if(Objects.nonNull(adaptiveThreadFactory)) {
      adaptiveThreadFactory.addParkingTime(System.nanoTime());
    }
  }

  // Transitioning

  /**
   * Comment
   *
   * @param thread Comment
   * @return Comment
   */
  public static boolean isMarkedForTransition(Thread thread) {
    return associations.get(thread).getMarks().get(thread);
  }

  // Getters

  /**
   * Comment
   * @return Comment
   */
  public long getNumberParkingsInTimeWindow() {
    long lowerBound = System.nanoTime() - this.parkingTimeWindowLength;
    return countRecentAndRemoveOldValues(this.parkingTimes, lowerBound);
  }

  /**
   * Comment
   * @return Comment
   */
  public long getNumberThreadCreationsInTimeWindow() {
    long lowerBound = System.nanoTime() - this.threadCreationTimeWindowLength;
    return countRecentAndRemoveOldValues(this.threadCreationTimes, lowerBound);
  }

  /**
   * Comment
   * @return Comment
   */
  public long getNumberThreads() {
    return this.marks.size();
  }

  /**
   * Comment
   * @return Comment
   */
  public double getSystemCpuUsage() {
    return this.systemCpuUsageSupplier.get();
  }

  /**
   * Comment
   * @return Comment
   */
  public double getProcessCpuUsage() {
    return this.processCpuUsageSupplier.get();
  }

  /**
   * Comment
   * @return Comment
   */
  public Optional<ThreadType> getCurrentThreadType() {
    if (homogeneousExecutionModeInEffect()) {
      return Optional.of(this.currentThreadType);
    } else {
      return Optional.empty();
    }
  }

  // Setters

  /**
   * Comment
   *
   * @param parkingTimeWindowLength Comment
   */
  public void setParkingTimeWindowLength(long parkingTimeWindowLength) {
    this.parkingTimeWindowLength = parkingTimeWindowLength;
  }

  /**
   * Comment
   *
   * @param threadCreationTimeWindowLength Comment
   */
  public void setThreadCreationTimeWindowLength(
    long threadCreationTimeWindowLength
  ) {
    this.threadCreationTimeWindowLength = threadCreationTimeWindowLength;
  }

  /**
   * Comment
   *
   * @param threadTypeSelector Comment
   */
  public void setThreadTypeSelector(ThreadTypeSelector threadTypeSelector) {
    this.threadTypeSelector = threadTypeSelector;
  }

  /**
   * Comment
   *
   * @param cpuUsageSamplingPeriod Comment
   */
  public void setCpuUsageSamplingPeriod(int cpuUsageSamplingPeriod) {
    this.cpuUsageSamplingPeriod = cpuUsageSamplingPeriod;
  }

  /**
   * Comment
   *
   * @param numberRelevantCpuUsageSamples Comment
   */
  public void setNumberRelevantCpuUsageSamples(
    int numberRelevantCpuUsageSamples
  ) {
    this.numberRelevantCpuUsageSamples = numberRelevantCpuUsageSamples;
  }

  /**
   * Comment
   *
   * @param stateQueryInterval Comment
   */
  public void setStateQueryInterval(int stateQueryInterval) {
    this.stateQueryInterval = Optional.of(stateQueryInterval);
  }

  /**
   * Comment
   *
   * @param numberRecurrencesUntilTransition Comment
   */
  public void setNumberRecurrencesUntilTransition(
    int numberRecurrencesUntilTransition
  ) {
    this.numberRecurrencesUntilTransition =
      Optional.of(numberRecurrencesUntilTransition);
  }

  /**
   * Comment
   *
   * @param threadCreationHandler Comment
   */
  public void setThreadCreationHandler(Runnable threadCreationHandler) {
    this.threadCreationHandler = Optional.of(threadCreationHandler);
  }

  // User specification

  /**
   * Comment
   */
  public enum ThreadType {
    /** Comment */PLATFORM,
    /** Comment */VIRTUAL,
  }

  /**
   * Comment
   */
  @FunctionalInterface
  public interface ThreadTypeSelector {
    /**
     * Comment
     *
     * @param   numberThreadCreationsInTimeWindow Comment
     * @param   numberParkingsInTimeWindow Comment
     * @param   cpuUsage Comment
     * @param   numberThreads Comment
     * @param   currentThreadType Comment
     * @return Comment
     */
    ThreadType selectThreadType(
      long numberParkingsInTimeWindow,
      long numberThreadCreationsInTimeWindow,
      double cpuUsage,
      long numberThreads,
      Optional<ThreadType> currentThreadType
    );
  }

  /**
   * Comment
   */
  public static class AdaptiveThreadFactoryBuilder {

    private long parkingTimeWindowLength; // in milliseconds
    private long threadCreationTimeWindowLength; // in milliseconds
    private ThreadTypeSelector threadTypeSelector;
    private int cpuUsageSamplingPeriod;
    private int numberRelevantCpuUsageSamples;
    private Optional<Integer> stateQueryInterval; // in milliseconds
    private Optional<Integer> numberRecurrencesUntilTransition;
    private Optional<Runnable> threadCreationHandler;

    /**
     * Comment
     */
    public AdaptiveThreadFactoryBuilder() {
      this.parkingTimeWindowLength = getDefaultParkingTimeWindowLength();
      this.threadCreationTimeWindowLength =
        getDefaultThreadCreationTimeWindowLength();
      this.threadTypeSelector = getDefaultThreadTypeSelector();
      this.cpuUsageSamplingPeriod = getDefaultCpuUsageSamplingPeriod();
      this.numberRelevantCpuUsageSamples =
        getDefaultNumberRelevantCpuUsageSamples();
      this.stateQueryInterval = Optional.empty();
      this.numberRecurrencesUntilTransition = Optional.empty();
      this.threadCreationHandler = Optional.empty();
    }

    /**
     * Comment
     *
     * @param parkingTimeWindowLength Comment
     * @return Comment
     */
    public AdaptiveThreadFactoryBuilder setParkingTimeWindowLength(
      long parkingTimeWindowLength
    ) {
      this.parkingTimeWindowLength = parkingTimeWindowLength;
      return this;
    }

    /**
     * Comment
     *
     * @param threadCreationTimeWindowLength Comment
     * @return Comment
     */
    public AdaptiveThreadFactoryBuilder setThreadCreationTimeWindowLength(
      long threadCreationTimeWindowLength
    ) {
      this.threadCreationTimeWindowLength = threadCreationTimeWindowLength;
      return this;
    }

    /**
     * Comment
     *
     * @param threadTypeSelector Comment
     * @return Comment
     */
    public AdaptiveThreadFactoryBuilder setThreadTypeSelector(
      ThreadTypeSelector threadTypeSelector
    ) {
      this.threadTypeSelector = threadTypeSelector;
      return this;
    }

    /**
     * Comment
     *
     * @param cpuUsageSamplingPeriod Comment
     * @return Comment
     */
    public AdaptiveThreadFactoryBuilder setCpuUsageSamplingPeriod(
      int cpuUsageSamplingPeriod
    ) {
      this.cpuUsageSamplingPeriod = cpuUsageSamplingPeriod;
      return this;
    }

    /**
     * Comment
     *
     * @param numberRelevantCpuUsageSamples Comment
     * @return Comment
     */
    public AdaptiveThreadFactoryBuilder setNumberRelevantCpuUsageSamples(
      int numberRelevantCpuUsageSamples
    ) {
      this.numberRelevantCpuUsageSamples = numberRelevantCpuUsageSamples;
      return this;
    }

    /**
     * Comment
     *
     * @param stateQueryInterval Comment
     * @return Comment
     */
    public AdaptiveThreadFactoryBuilder setStateQueryInterval(
      int stateQueryInterval
    ) {
      this.stateQueryInterval = Optional.of(stateQueryInterval);
      return this;
    }

    /**
     * Comment
     *
     * @param numberRecurrencesUntilTransition Comment
     * @return Comment
     */
    public AdaptiveThreadFactoryBuilder setNumberRecurrencesUntilTransition(
      int numberRecurrencesUntilTransition
    ) {
      this.numberRecurrencesUntilTransition =
        Optional.of(numberRecurrencesUntilTransition);
      return this;
    }

    /**
     * Comment
     *
     * @param threadCreationHandler Comment
     * @return Comment
     */
    public AdaptiveThreadFactoryBuilder setThreadCreationHandler(
      Runnable threadCreationHandler
    ) {
      this.threadCreationHandler = Optional.of(threadCreationHandler);
      return this;
    }

    /**
     * Comment
     *
     * @return Comment
     */
    public AdaptiveThreadFactory build() {
      AdaptiveThreadFactory adaptiveThreadFactory = new AdaptiveThreadFactory(
        this.parkingTimeWindowLength,
        this.threadCreationTimeWindowLength,
        this.threadTypeSelector,
        this.cpuUsageSamplingPeriod,
        this.numberRelevantCpuUsageSamples,
        this.stateQueryInterval,
        this.numberRecurrencesUntilTransition,
        this.threadCreationHandler
      );
      return adaptiveThreadFactory;
    }
  }
}
