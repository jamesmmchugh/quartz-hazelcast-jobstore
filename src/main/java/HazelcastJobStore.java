import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionOptions.TransactionType;

import org.quartz.*;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.spi.*;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by James on 08/09/2014.
 */
public class HazelcastJobStore implements JobStore {

	TransactionalMap<JobKey, JobDetail> jobMap;
	TransactionalMap<TriggerKey, Trigger> triggerMap;
	ILock lock;

	private HazelcastInstance hazelcastInstance;
	private TransactionContext transactionContext;

	private String instanceId;
	private String instanceName;

	@Override
    public void initialize(ClassLoadHelper classLoadHelper, SchedulerSignaler schedulerSignaler) throws SchedulerConfigException {

    }

    @Override
    public void schedulerStarted() throws SchedulerException {
        //TODO Setup required maps in hazelcast
		TransactionOptions options = new TransactionOptions()
				.setTransactionType( TransactionType.LOCAL );

		transactionContext = hazelcastInstance.newTransactionContext(options);
//
//		TransactionalQueue queue = transactionContext.getQueue("myqueue");
//		TransactionalMap map = transactionContext.getMap("mymap");
//		TransactionalSet set = transactionContext.getSet("myset");
    }

    @Override
    public void schedulerPaused() {
        //Do nothing
    }

    @Override
    public void schedulerResumed() {
        //Do nothing
    }

    @Override
    public void shutdown() {
        //Shutdown hazelcast
    }

    @Override
    public boolean supportsPersistence() {
        return true;
    }

    @Override
    public long getEstimatedTimeToReleaseAndAcquireTrigger() {
        //TODO what is this?
        return 100;
    }

    @Override
    public boolean isClustered() {
        return true;
    }

	private void handleException(Exception e) throws JobPersistenceException {
		if(e instanceof ObjectAlreadyExistsException) {
			throw (ObjectAlreadyExistsException) e;
		}
		throw new JobPersistenceException(e.getMessage(), e);
	}

    @Override
    public void storeJobAndTrigger(JobDetail jobDetail, OperableTrigger operableTrigger) throws ObjectAlreadyExistsException, JobPersistenceException {
		lock.lock();
		transactionContext.beginTransaction();
		checkJobAlreadyExists(jobDetail);
		try {
			storeJob(jobDetail, false);
			storeTrigger(operableTrigger, false);
			transactionContext.commitTransaction();
		} catch (Exception e) {
			transactionContext.rollbackTransaction();
			handleException(e);
		} finally {
			lock.unlock();
		}
    }

	@Override
    public void storeJob(JobDetail jobDetail, boolean replaceExisting) throws ObjectAlreadyExistsException, JobPersistenceException {
		lock.lock();
		transactionContext.beginTransaction();
		try {
			if (replaceExisting) {
				checkJobAlreadyExists(jobDetail);
			}
			jobMap.put(jobDetail.getKey(), jobDetail);
			transactionContext.commitTransaction();
		} catch (Exception e) {
			transactionContext.rollbackTransaction();
			handleException(e);
		} finally {
			lock.unlock();
		}
    }

    @Override
    public void storeJobsAndTriggers(Map<JobDetail, Set<? extends Trigger>> jobDetailSetMap, boolean replaceExisting) throws ObjectAlreadyExistsException, JobPersistenceException {
		lock.lock();
		transactionContext.beginTransaction();
		try {
			if(!replaceExisting) {
				checkForExistingJobsOrTriggers(jobDetailSetMap);
			}
			for (JobDetail jobDetail : jobDetailSetMap.keySet()) {
				storeJob(jobDetail, true);

				for(Trigger trigger : jobDetailSetMap.get(jobDetail)) {
					storeTrigger((OperableTrigger) trigger, true);
				}
			}
		} catch (Exception e) {
			transactionContext.rollbackTransaction();
			handleException(e);
		} finally {
			lock.unlock();
		}
	}

	private void checkForExistingJobsOrTriggers(Map<JobDetail, Set<? extends Trigger>> jobDetailSetMap) throws JobPersistenceException {
		for (JobDetail jobDetail : jobDetailSetMap.keySet()) {
			checkJobAlreadyExists(jobDetail);
			for(Trigger trigger : jobDetailSetMap.get(jobDetail)) {
				checkTriggerAlreadyExists(trigger);
			}
		}
	}

	private void checkTriggerAlreadyExists(Trigger trigger) throws JobPersistenceException {
		if(checkExists(trigger.getKey())) {
			throw new ObjectAlreadyExistsException(trigger);
		}
	}

	private void checkJobAlreadyExists(JobDetail jobDetail) throws JobPersistenceException {
		if(checkExists(jobDetail.getKey())) {
			throw new ObjectAlreadyExistsException(jobDetail);
		}
	}

	@Override
    public boolean removeJob(JobKey jobKey) throws JobPersistenceException {
		lock.lock();
		transactionContext.beginTransaction();
		boolean removed = false;
		try {
			removed = jobMap.remove(jobKey) != null;
			transactionContext.commitTransaction();
		} catch(Exception e) {
			transactionContext.rollbackTransaction();
			handleException(e);
		} finally {
			lock.unlock();
		}
		return removed;
    }

    @Override
    public boolean removeJobs(List<JobKey> jobKeys) throws JobPersistenceException {
		lock.lock();
		transactionContext.beginTransaction();
		boolean removed = true;
		try {
			for(JobKey jobKey : jobKeys) {
				removed = removed && removeJob(jobKey);
			}
			transactionContext.commitTransaction();
		} catch(Exception e) {
			transactionContext.rollbackTransaction();
			handleException(e);
		} finally {
			lock.unlock();
		}
		return removed;
    }

    @Override
    public JobDetail retrieveJob(JobKey jobKey) throws JobPersistenceException {
        lock.lock();
		JobDetail jobDetail = null;
		try {
			jobDetail = jobMap.get(jobKey);
		} catch (Exception e) {
			handleException(e);
		} finally {
			lock.unlock();
		}
		return jobDetail;
    }

    @Override
    public void storeTrigger(OperableTrigger operableTrigger, boolean replaceExisting) throws ObjectAlreadyExistsException, JobPersistenceException {
		if(triggerMap.containsKey(operableTrigger.getKey()) && !replaceExisting) {
			throw new ObjectAlreadyExistsException(operableTrigger);
		}
		triggerMap.put(operableTrigger.getKey(), operableTrigger);
    }

    @Override
    public boolean removeTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        return false;
    }

    @Override
    public boolean removeTriggers(List<TriggerKey> triggerKeys) throws JobPersistenceException {
        return false;
    }

    @Override
    public boolean replaceTrigger(TriggerKey triggerKey, OperableTrigger operableTrigger) throws JobPersistenceException {
        return false;
    }

    @Override
    public OperableTrigger retrieveTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        return null;
    }

    @Override
    public boolean checkExists(JobKey jobKey) throws JobPersistenceException {
        return false;
    }

    @Override
    public boolean checkExists(TriggerKey triggerKey) throws JobPersistenceException {
        return false;
    }

    @Override
    public void clearAllSchedulingData() throws JobPersistenceException {

    }

    @Override
    public void storeCalendar(String s, Calendar calendar, boolean b, boolean b2) throws ObjectAlreadyExistsException, JobPersistenceException {

    }

    @Override
    public boolean removeCalendar(String s) throws JobPersistenceException {
        return false;
    }

    @Override
    public Calendar retrieveCalendar(String s) throws JobPersistenceException {
        return null;
    }

    @Override
    public int getNumberOfJobs() throws JobPersistenceException {
        return 0;
    }

    @Override
    public int getNumberOfTriggers() throws JobPersistenceException {
        return 0;
    }

    @Override
    public int getNumberOfCalendars() throws JobPersistenceException {
        return 0;
    }

    @Override
    public Set<JobKey> getJobKeys(GroupMatcher<JobKey> jobKeyGroupMatcher) throws JobPersistenceException {
        return null;
    }

    @Override
    public Set<TriggerKey> getTriggerKeys(GroupMatcher<TriggerKey> triggerKeyGroupMatcher) throws JobPersistenceException {
        return null;
    }

    @Override
    public List<String> getJobGroupNames() throws JobPersistenceException {
        return null;
    }

    @Override
    public List<String> getTriggerGroupNames() throws JobPersistenceException {
        return null;
    }

    @Override
    public List<String> getCalendarNames() throws JobPersistenceException {
        return null;
    }

    @Override
    public List<OperableTrigger> getTriggersForJob(JobKey jobKey) throws JobPersistenceException {
        return null;
    }

    @Override
    public Trigger.TriggerState getTriggerState(TriggerKey triggerKey) throws JobPersistenceException {
        return null;
    }

    @Override
    public void pauseTrigger(TriggerKey triggerKey) throws JobPersistenceException {

    }

    @Override
    public Collection<String> pauseTriggers(GroupMatcher<TriggerKey> triggerKeyGroupMatcher) throws JobPersistenceException {
        return null;
    }

    @Override
    public void pauseJob(JobKey jobKey) throws JobPersistenceException {

    }

    @Override
    public Collection<String> pauseJobs(GroupMatcher<JobKey> jobKeyGroupMatcher) throws JobPersistenceException {
        return null;
    }

    @Override
    public void resumeTrigger(TriggerKey triggerKey) throws JobPersistenceException {

    }

    @Override
    public Collection<String> resumeTriggers(GroupMatcher<TriggerKey> triggerKeyGroupMatcher) throws JobPersistenceException {
        return null;
    }

    @Override
    public Set<String> getPausedTriggerGroups() throws JobPersistenceException {
        return null;
    }

    @Override
    public void resumeJob(JobKey jobKey) throws JobPersistenceException {

    }

    @Override
    public Collection<String> resumeJobs(GroupMatcher<JobKey> jobKeyGroupMatcher) throws JobPersistenceException {
        return null;
    }

    @Override
    public void pauseAll() throws JobPersistenceException {

    }

    @Override
    public void resumeAll() throws JobPersistenceException {

    }

    @Override
    public List<OperableTrigger> acquireNextTriggers(long l, int i, long l2) throws JobPersistenceException {
        return null;
    }

    @Override
    public void releaseAcquiredTrigger(OperableTrigger operableTrigger) {

    }

    @Override
    public List<TriggerFiredResult> triggersFired(List<OperableTrigger> operableTriggers) throws JobPersistenceException {
        return null;
    }

    @Override
    public void triggeredJobComplete(OperableTrigger operableTrigger, JobDetail jobDetail, Trigger.CompletedExecutionInstruction completedExecutionInstruction) {

    }

    @Override
    public void setInstanceId(String instanceId) {
		this.instanceId = instanceId;
    }

    @Override
    public void setInstanceName(String instanceName) {
		this.instanceName = instanceName;
    }

    @Override
    public void setThreadPoolSize(int i) {
		//NA
    }
}
