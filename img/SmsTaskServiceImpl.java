package com.cictek.mall.portal.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.thread.ThreadUtil;
import com.baomidou.lock.LockInfo;
import com.baomidou.lock.LockTemplate;
import com.baomidou.lock.executor.RedisTemplateLockExecutor;
import com.cictek.mall.common.api.CommonPage;
import com.cictek.mall.common.exception.Asserts;
import com.cictek.mall.mapper.SmsMemberTaskMapper;
import com.cictek.mall.mapper.SmsTaskMapper;
import com.cictek.mall.mapper.UmsMemberConsumptionMapper;
import com.cictek.mall.model.*;
import com.cictek.mall.portal.domain.SmsTaskInfo;
import com.cictek.mall.portal.service.MemberPointsService;
import com.cictek.mall.portal.service.SmsTaskService;
import com.cictek.mall.portal.service.UmsMemberService;
import com.cictek.mall.portal.util.DateUtil;
import com.github.pagehelper.PageHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.mybatis.generator.internal.util.StringUtility;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class SmsTaskServiceImpl implements SmsTaskService {
    @Resource
    UmsMemberService memberService;
    @Resource
    SmsTaskMapper taskMapper;
    @Resource
    SmsMemberTaskMapper memberTaskMapper;
    @Resource
    MemberPointsService memberPointsService;
    @Resource
    UmsMemberConsumptionMapper umsMemberConsumptionMapper;
    @Resource
    LockTemplate lockTemplate;

    // 查看当前日期: 是否超过输入的日期：
    //
    private Boolean checkEndTimeExpired(Date endTime) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd 00:00:00");
        String currentTime = sdf.format(new Date());
        Date currentDate = new Date();

        //Date infoEndTime = sdf.parse(endTime);
        // 如果当前时间 > 最终时间，则跳出, 不再显示.
        if (currentDate.compareTo(endTime) >= 0) {
            return true;
        }
        else {
            return false;
        }
    }

    private Boolean checkEndTimeExpired(String endTime) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String currentTime = sdf.format(new Date());
        Date currentDate = new Date();

        if (StringUtils.isEmpty(endTime)) return false;

        Date infoEndTime = sdf.parse(endTime);
        // 如果当前时间 > 最终时间，则跳出, 不再显示.
        if (currentDate.compareTo(infoEndTime) >= 0) {
            return true;
        }
        else {
            return false;
        }
    }

    // 查找
    public SmsMemberTask findMemberTask(SmsTask task, List<SmsMemberTask> memberTasks) {
        for (SmsMemberTask memberTask : memberTasks) {
            if (task.getId().equals(memberTask.getTaskid())) {
                return memberTask;
            }
        }
        SmsMemberTask memberTask = new SmsMemberTask();
        BeanUtil.copyProperties(task, memberTask);
        memberTask.setStatus(0);
        return memberTask;
    }

    @Override
    public List<SmsTaskInfo> listAll() throws ParseException {
        //ThreadUtil.sleep(5000); //测试延迟

        // TODO: 这里需要增加周期性任务的判断.
        //      例如： 每日均可完成的任务，第二天的状态应该是要刷新过来的了。

        // TODO: 2021-04-01:
        //      增加 task的排序功能，按照sort字段进行排序.

        // 根据会员，获取会员已经做过的任务列表.
        UmsMember member = memberService.getCurrentMember();
        SmsMemberTaskExample example = new SmsMemberTaskExample();
        example.createCriteria().andMemberidEqualTo(member.getId());
        List<SmsMemberTask> memberTaskList = memberTaskMapper.selectByExample(example);
        Calendar calendar = Calendar.getInstance();

        log.info("memberTaskList: size = {}", memberTaskList.size());

        // 过滤任务: 如果任务截至时间:
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String currentTime = sdf.format(new Date());
        Date currentDate = new Date();

        SmsTaskExample taskExample = new SmsTaskExample();
        SmsTaskExample.Criteria criteria = taskExample.createCriteria();

        // 过滤: (publicStatus = 1 and EndTime is null)
        criteria.andPublishStatusEqualTo(1).andEndTimeIsNull();

        // 过滤: (publicStatus=1 and EndTime = '')
        taskExample.or().andPublishStatusEqualTo(1).andEndTimeEqualTo("");

        // 过滤任务: 如果任务截至时间:
        taskExample.or().andEndTimeGreaterThan(currentTime).andPublishStatusEqualTo(1);


        taskExample.setOrderByClause("sort desc");

        // 取出整个任务列表.
        List<SmsTask> taskList = taskMapper.selectByExample(taskExample);

        // 这是需要返回的任务列表（类型已改变).
        List<SmsTaskInfo> infoList = new ArrayList<>();
        for (SmsTask task: taskList) {
            SmsTaskInfo info = new SmsTaskInfo();
            BeanUtil.copyProperties(task, info);

            //------------------------------------------------------------------------
            // 对比: 当前任务，与成员的历史任务：做比较:
            log.info("info = {}", info.toString());
            log.info("info.getLoopType() = {}", info.getLoopType());

            // 如果任务有开始日期，则增加判断: 当前日期未到开始日期:
            if (StringUtils.isNotEmpty(info.getStartTime())) {
                Date startTime = sdf.parse(info.getStartTime());
                if (currentDate.before(startTime)) {
                    continue;
                }
            }

            // 如果任务有截至日期：则判断: // 如果已经过期，则不显示了，直接继续
            if (StringUtils.isNotEmpty(info.getEndTime())) {
                if (checkEndTimeExpired(info.getEndTime())) {
                    continue;
                }
            }

            // 非循环任务，无有效期
            if ((info.getLoopType() == null || info.getLoopType()==0)
                    && StringUtils.isEmpty(info.getEndTime())) {
                SmsMemberTask memberTask = findMemberTask(task, memberTaskList);
                info.setMemberStatus(memberTask.getStatus());
                infoList.add(info);
                continue;
            }

            // 非循环任务，带有效期:
            if ((info.getLoopType() == null && StringUtils.isNotEmpty(info.getEndTime())) ||
                    (info.getLoopType() != null && info.getLoopType().equals(0) && info.getEndTime() != null)) {
                // 如果未过期，则显示
                if (checkEndTimeExpired(info.getEndTime()) == false) {
                    SmsMemberTask memberTask = findMemberTask(task, memberTaskList);
                    info.setMemberStatus(memberTask.getStatus());
                    infoList.add(info);
                }
                continue;
            }

            // 运行到这里: 肯定是周期性任务. 并且只校验上一次完成的时间即可.
            SmsMemberTask memberTask = findMemberTask(task, memberTaskList);
            Date lastTime = memberTask.getLastTime();
            if (lastTime == null) {
                infoList.add(info);
                continue;
            }

            // 如果非空
            calendar.setTime(lastTime);
            Date getTime = calendar.getTime();
            Date startTime = lastTime;

            switch (info.getLoopType()) {
                case 2: // 周
                    //calendar.add(Calendar.DAY_OF_WEEK, 1);
                    lastTime = DateUtil.getNextMonday(calendar.getTime(), info.getLoopPeriod());
                    break;
                case 3: // 月
                    //calendar.add(Calendar.MONTH, 1);
                    lastTime = DateUtil.firstDayOfNextMonth(calendar.getTime(), info.getLoopPeriod());
                    break;
                case 4: // 年
                    //calendar.add(Calendar.YEAR, 1);
                    lastTime = DateUtil.firstDayOfNextYear(calendar.getTime(), info.getLoopPeriod());
                    break;
                default:
                case 1: // 日
                    calendar.add(Calendar.DATE, info.getLoopPeriod());
                    lastTime = calendar.getTime();
                    break;
            }
            log.info("---- taskId= {}, lastTime = {}, getTime = {}", task.getId(), lastTime, getTime);
            log.info("循环数: {} ", info.getLoopPeriod());

            // 用现在的时间： 与最后一次task时间相比:
            log.info("currentTime = {}", currentDate);

            startTime = cn.hutool.core.date.DateUtil.beginOfDay(startTime);
            lastTime = cn.hutool.core.date.DateUtil.beginOfDay(lastTime);

            // 如果已经是完成过，则显示：已经完成
            if (currentDate.after(startTime) && currentDate.before(lastTime)) {
                info.setMemberStatus(memberTask.getStatus());
            }
            else if (currentDate.after(lastTime)) {
                info.setMemberStatus(0);
            }

            infoList.add(info);
        }

        return infoList;
    }

//    @Override
//    public Map<String, Object> beginTask(long taskId) throws ParseException {
//        // 判断当前任务类型，推广型任务，触发一次即算完成，资格型任务，需要判断资格状态
//        UmsMember member = memberService.getCurrentMember();
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//
//        SmsTask smsTask = taskMapper.selectByPrimaryKey(taskId);
//        if (smsTask == null){
//            Asserts.fail("没有找到该活动");
//        }
//
//        // 任务的截至时间:
//        Date currentTime = new Date();
//
//        //如果非空, 进行任务的endTime 截至时间进行比较.
//        //String endTime = smsTask.getEndTime();
//        if (StringUtils.isNotBlank(smsTask.getEndTime())) {
//            Date taskEndTime = sdf.parse(smsTask.getEndTime());
//            if (taskEndTime.compareTo(currentTime) <= 0) {
//                Asserts.fail("该活动已经结束.");
//            }
//        }
//
//        Map<String, Object> result = new HashMap<>();
//
//        SmsMemberTaskExample example = new SmsMemberTaskExample();
//        example.createCriteria().andMemberidEqualTo(member.getId()).andTaskidEqualTo(taskId);
//        List<SmsMemberTask> memberTaskList = memberTaskMapper.selectByExample(example);
//        SmsMemberTask smsMemberTask = null;
//        if (CollectionUtils.isEmpty(memberTaskList)){
//            smsMemberTask = new SmsMemberTask();
//            smsMemberTask.setMemberid(member.getId());
//            smsMemberTask.setTaskid(smsTask.getId());
//        }
//        // 如果是非周期性任务：直接判断
//        else if (smsTask.getLoopType() == 0){
//            smsMemberTask = memberTaskList.get(0);
//            if (smsMemberTask.getStatus() != 0){
//                Asserts.fail("任务已经做过了");
//            }
//        }
//        else if (smsTask.getLoopType() != 0) {
//            smsMemberTask = memberTaskList.get(0);
//            // 再次判断周期性任务:
//            // if (currentTime.compareTo())
//            Date lastTime = smsMemberTask.getLastTime();
//            Calendar calendar = Calendar.getInstance();
//            calendar.setTime(lastTime);
//            switch (smsTask.getLoopType()) {
//                case 2: // 周
//                    //calendar.add(Calendar.DAY_OF_WEEK, 1);
//                    lastTime = DateUtil.getNextMonday(calendar.getTime());
//                    break;
//                case 3: // 月
//                    //calendar.add(Calendar.MONTH, 1);
//                    lastTime = DateUtil.firstDayOfNextMonth(calendar.getTime());
//                    break;
//                case 4: // 年
//                    //calendar.add(Calendar.YEAR, 1);
//                    lastTime = DateUtil.firstDayOfNextYear(calendar.getTime());
//                    break;
//                default:
//                case 1: // 日
//                    calendar.add(Calendar.DATE, 1);
//                    lastTime = calendar.getTime();
//                    break;
//            }
//            // 用现在的时间： 与最后一次task时间相比, 时间还未到.
//            if (currentTime.compareTo(lastTime)<0) {
//                Asserts.fail("任务的开始时间还未到.");
//            }
//        }
//
//        if (smsTask.getType() == 0) {//推广型任务
//            int points = memberPointsService.incPoints(member.getUniqueUserId(), smsTask.getPoints(), "完成任务:"+smsTask.getName());
//            smsMemberTask.setStatus(2);
//            memberTaskMapper.updateByPrimaryKey(smsMemberTask);
//
//            result.put("points", points);
//            result.put("incPoints", smsTask.getPoints());
//        }else {
//            smsMemberTask.setStatus(1); //待完成
//        }
//
//        smsMemberTask.setLastTime(new Date());
//        if (CollectionUtils.isEmpty(memberTaskList)) {
//            memberTaskMapper.insert(smsMemberTask);
//        } else {
//            memberTaskMapper.updateByPrimaryKey(smsMemberTask);
//        }
//
//        SmsTaskInfo info = new SmsTaskInfo();
//        BeanUtils.copyProperties(smsTask, info);
//        info.setMemberStatus(smsMemberTask.getStatus());
//        result.put("info", info);
//        return result;
//    }

//    @Override
//    public Map<String, Object> endTask(long taskId) throws ParseException {
//        UmsMember member = memberService.getCurrentMember();
//
//        SmsTask smsTask = taskMapper.selectByPrimaryKey(taskId);
//        if (smsTask == null){
//            Asserts.fail("没有找到该活动");
//        }
//
//        //如果非空, 进行任务的endTime 截至时间进行比较.
//        //String endTime = smsTask.getEndTime();
//        if (StringUtils.isNotBlank(smsTask.getEndTime())) {
//            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//            Date endTime = sdf.parse(smsTask.getEndTime());
//            Date currentTime = new Date();
//            if (endTime.compareTo(currentTime) <= 0) {
//                Asserts.fail("该活动已经结束.");
//            }
//        }
//
//        SmsMemberTaskExample example = new SmsMemberTaskExample();
//        example.createCriteria().andMemberidEqualTo(member.getId()).andTaskidEqualTo(taskId);
//        List<SmsMemberTask> memberTaskList = memberTaskMapper.selectByExample(example);
//        if (CollectionUtils.isEmpty(memberTaskList)){
//            Asserts.fail("需要去完成该任务");
//        }
//
//        SmsMemberTask smsMemberTask  = memberTaskList.get(0);
//        if (smsMemberTask.getStatus() != 1){
//            Asserts.fail("任务已经完成");
//        }
//
//        Map<String, Object> result = new HashMap<>();
//        //...检查任务是否完成,完成领取积分, 如果没有完成，并且离上次操作时间间隔大于60分钟，状态重置为0
//        boolean finished =  memberPointsService.queryTaskStatus(member.getUniqueUserId(), member.getPhone(), member.getPersonalId(), smsTask.getZgzid());
//        if (finished) {
//            int points = memberPointsService.incPoints(member.getUniqueUserId(), smsTask.getPoints(), "完成任务:"+smsTask.getName());
//            smsMemberTask.setStatus(2);
//            memberTaskMapper.updateByPrimaryKey(smsMemberTask);
//
//            result.put("points", points);
//            result.put("incPoints", smsTask.getPoints());
//        } else{
//            //超过指定时间状态还没有变成完成，自动转变成去完成
////            Date today = new Date();
////            if ((today.getTime() - smsMemberTask.getLastTime().getTime()) > 60 * 60 * 1000){
////                smsMemberTask.setStatus(0);
////                memberTaskMapper.updateByPrimaryKey(smsMemberTask);
////            }
//        }
//
//        SmsTaskInfo info = new SmsTaskInfo();
//        BeanUtils.copyProperties(smsTask, info);
//        info.setMemberStatus(smsMemberTask.getStatus());
//        result.put("info", info);
//        return result;
//    }

    @Override
    public Map<String, Object> dealTask(long taskId) throws ParseException {
        Map<String, Object> result = new HashMap<>();

        UmsMember member = memberService.getCurrentMember();
        // 任务的截至时间:
        Date currentTime = new Date();
        // 可使用任务
        SmsTask smsTask = taskMapper.selectByPrimaryKey(taskId);
        if (smsTask == null){
            Asserts.fail("没有找到该活动");
        }

        log.info("dealTask: 检查资格组ID： zgzId = {}", smsTask.getZgzid());

        //如果非空, 进行任务的endTime 截至时间进行比较.
        //String endTime = smsTask.getEndTime();
        if (StringUtils.isNotBlank(smsTask.getEndTime())) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date endTime = sdf.parse(smsTask.getEndTime());
            //Date currentTime = new Date();
            if (endTime.compareTo(currentTime) <= 0) {
                Asserts.fail("该活动已经结束.");
            }
        }

        // 获取锁
        String lockerId = member.getUniqueUserId() + "-" + String.valueOf(taskId);
        final LockInfo lockInfo = lockTemplate.lock(lockerId, 10000L, 5000L, RedisTemplateLockExecutor.class);
        if (null == lockInfo) {
            log.error("获取锁异常",e);
            Asserts.fail("您操作过于频繁，请稍后再试");
//            throw new RuntimeException("业务处理中,请稍后再试");
        }

        // 当前会员任务状态
        SmsMemberTaskExample example = new SmsMemberTaskExample();
        example.createCriteria().andMemberidEqualTo(member.getId()).andTaskidEqualTo(taskId);
        List<SmsMemberTask> memberTaskList = memberTaskMapper.selectByExample(example);
        boolean update = true;
        SmsMemberTask smsMemberTask = null;
        if (CollectionUtils.isEmpty(memberTaskList)){
            smsMemberTask = new SmsMemberTask();
            smsMemberTask.setMemberid(member.getId());
            smsMemberTask.setTaskid(smsTask.getId());
            smsMemberTask.setStatus(0);
            update = false;
        }
        // 如果是非周期性任务：直接判断
        else if (smsTask.getLoopType() == 0){
            smsMemberTask = memberTaskList.get(0);
            if (smsMemberTask.getStatus() == 2){
                Asserts.fail("任务已经做过了");
            }
        }
        else if (smsTask.getLoopType() != 0) {
            // 再次判断周期性任务:
            smsMemberTask = memberTaskList.get(0);

            // 如果任务已经完成, 才需要判断有效期；如果 status = 0 或者1，不需要判断, 直接
            if (smsMemberTask.getStatus().equals(2)) {
                Date lastTime = smsMemberTask.getLastTime();
                Calendar calendar = Calendar.getInstance();
                calendar.setTime(lastTime);
                switch (smsTask.getLoopType()) {
                    case 2: // 周
                        //calendar.add(Calendar.DAY_OF_WEEK, 1);
                        lastTime = DateUtil.getNextMonday(calendar.getTime(), smsTask.getLoopPeriod());
                        break;
                    case 3: // 月
                        //calendar.add(Calendar.MONTH, 1);
                        lastTime = DateUtil.firstDayOfNextMonth(calendar.getTime(), smsTask.getLoopPeriod());
                        break;
                    case 4: // 年
                        //calendar.add(Calendar.YEAR, 1);
                        lastTime = DateUtil.firstDayOfNextYear(calendar.getTime(), smsTask.getLoopPeriod());
                        break;
                    default:
                    case 1: // 日
                        calendar.add(Calendar.DATE, smsTask.getLoopPeriod());
                        lastTime = calendar.getTime();
                        break;
                }

                lastTime = cn.hutool.core.date.DateUtil.beginOfDay(lastTime);

                // 用现在的时间： 与最后一次task时间相比, 时间还未到.
                if (currentTime.compareTo(lastTime)<0) {
                    Asserts.fail("任务的开始时间还未到.");
                }
            }
        }

        if (smsTask.getType() == 0) { /*推广型任务*/
            // 去完成
            //if (smsMemberTask.getStatus() == 0)
            //{ // 周期性任务,不考虑这个状态了?
                smsMemberTask.setStatus(2);
                smsMemberTask.setLastTime(new Date());
                if (update){
                    memberTaskMapper.updateByPrimaryKey(smsMemberTask);
                } else {
                    memberTaskMapper.insert(smsMemberTask);
                }

                int points = memberPointsService.incPoints(member.getUniqueUserId(), smsTask.getPoints(), "完成任务:"+smsTask.getName());
                log.info("推广型任务: 直接增加积分: points = {}", points);
            //记录积分
            UmsMemberConsumption umsMemberConsumption = new UmsMemberConsumption();
            umsMemberConsumption.setMemberId(""+member.getId());
            umsMemberConsumption.setProcessingTime(new Date());
            umsMemberConsumption.setDetails("完成任务:" + smsTask.getName());
            umsMemberConsumption.setType("1");
            umsMemberConsumption.setPoints(smsTask.getPoints());
            umsMemberConsumption.setBalance(points);
            umsMemberConsumptionMapper.insert(umsMemberConsumption);
                result.put("points", points);
                result.put("incPoints", smsTask.getPoints());
            //}
        } else if (smsTask.getType() == 1){ /*资格型任务*/
            //...检查任务是否完成,完成领取积分, 如果没有完成，并且离上次操作时间间隔大于60分钟，状态重置为0
            boolean finished =  memberPointsService.queryTaskStatus(member.getUniqueUserId(), member.getPhone(), member.getPersonalId(), smsTask.getZgzid());
            log.info("资格型任务: 从接口获取任务完整状态: finished = {}", finished);
            // 周期性任务: 直接算积分.
            if (finished) { // finished
                smsMemberTask.setStatus(2);
                smsMemberTask.setLastTime(new Date());
                if (update){
                    memberTaskMapper.updateByPrimaryKey(smsMemberTask);
                } else {
                    memberTaskMapper.insert(smsMemberTask);
                }

                log.info("完成任务 {}, 增加积分: {}, update = {}", smsTask.getName(), smsTask.getPoints(), update);
                int points = memberPointsService.incPoints(member.getUniqueUserId(), smsTask.getPoints(), "完成任务:" + smsTask.getName());

                //记录积分
                UmsMemberConsumption umsMemberConsumption = new UmsMemberConsumption();
                umsMemberConsumption.setMemberId(""+member.getId());
                umsMemberConsumption.setProcessingTime(new Date());
                umsMemberConsumption.setDetails("完成任务:" + smsTask.getName());
                umsMemberConsumption.setType("1");
                umsMemberConsumption.setPoints(smsTask.getPoints());
                umsMemberConsumption.setBalance(points);
                umsMemberConsumptionMapper.insert(umsMemberConsumption);

                result.put("points", points);
                result.put("incPoints", smsTask.getPoints());
            }
            else {
                log.info("资格组 Task 状态判断 11：  status = {}", smsMemberTask.getStatus());

                if (smsMemberTask.getStatus() == 0){
                    smsMemberTask.setStatus(1); //待完成
                    smsMemberTask.setLastTime(new Date());
                    if (update){
                        memberTaskMapper.updateByPrimaryKey(smsMemberTask);
                    } else {
                        memberTaskMapper.insert(smsMemberTask);
                    }
                }
            }
        } else {
            Asserts.fail("未定义类型任务");
        }

        log.info("资格组 Task 状态判断 22：  status = {}", smsMemberTask.getStatus());

        SmsTaskInfo info = new SmsTaskInfo();
        BeanUtils.copyProperties(smsTask, info);
        info.setMemberStatus(smsMemberTask.getStatus());
        result.put("info", info);
        return result;
    }

}
