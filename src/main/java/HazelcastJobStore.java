import com.hazelcast.core.IMap;
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

    IMap<JobKey, JobDetail> jobMap;
    IMap<TriggerKey, Trigger> triggerMap;

    @Override
    public void initialize(ClassLoadHelper classLoadHelper, SchedulerSignaler schedulerSignaler) throws SchedulerConfigException {

    }

    @Override
    public void schedulerStarted() throws SchedulerException {
        //TODO Setup required maps in hazelcast
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
        //FIXME make optional
        return true;
    }

    @Override
    public long getEstimatedTimeToReleaseAndAcquireTrigger() {
        //TODO what is this?
        return 0;
    }

    @Override
    public boolean isClustered() {
        return true;
    }

    //Map of OperableTrigger -> JobDetail
    //Map of JobKey -> JobDetail
    //List of JobDetails (without triggers)
    //List of OperableTrigger (without jobs)
    //Map TriggerKey -> OperableTrigger
    //Map String -> Calendar

    //JobKey -> JobDetail
    //TriggerKey -> TriggerDetail

    @Override
    public void storeJobAndTrigger(JobDetail jobDetail, OperableTrigger operableTrigger) throws ObjectAlreadyExistsException, JobPersistenceException {
        if(jobMap.containsKey(jobDetail.getKey())) {
            throw new ObjectAlreadyExistsException(jobDetail);
        }
        jobMap.put(jobDetail.getKey(), jobDetail);
        triggerMap.put(operableTrigger.getKey(), operableTrigger);
    }

    @Override
    public void storeJob(JobDetail jobDetail, boolean replaceExisting) throws ObjectAlreadyExistsException, JobPersistenceException {
        if(replaceExisting && jobMap.containsKey(jobDetail.getKey())) {
            throw new ObjectAlreadyExistsException(jobDetail);
        }
        jobMap.put(jobDetail.getKey(), jobDetail);
    }

    @Override
    public void storeJobsAndTriggers(Map<JobDetail, Set<? extends Trigger>> jobDetailSetMap, boolean replaceExisting) throws ObjectAlreadyExistsException, JobPersistenceException {
        //TODO what is difference between Trigger and OeperableTrigger, look at examples?
    }

    @Override
    public boolean removeJob(JobKey jobKey) throws JobPersistenceException {
        return false;
    }

    @Override
    public boolean removeJobs(List<JobKey> jobKeys) throws JobPersistenceException {
        return false;
    }

    @Override
    public JobDetail retrieveJob(JobKey jobKey) throws JobPersistenceException {
        return null;
    }

    @Override
    public void storeTrigger(OperableTrigger operableTrigger, boolean b) throws ObjectAlreadyExistsException, JobPersistenceException {

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
    public void setInstanceId(String s) {

    }

    @Override
    public void setInstanceName(String s) {

    }

    @Override
    public void setThreadPoolSize(int i) {

    }
}
