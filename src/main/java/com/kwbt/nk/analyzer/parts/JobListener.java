package com.kwbt.nk.analyzer.parts;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;

public class JobListener extends JobExecutionListenerSupport {

	private final static Logger logger = LoggerFactory.getLogger(JobListener.class.getName());

	@Override
	public void beforeJob(JobExecution jobExecution) {
		super.beforeJob(jobExecution);

		logger.info("start job: {} {}", jobExecution.getJobId().toString(), jobExecution.getJobConfigurationName());
	}

	@Override
	public void afterJob(JobExecution jobExecution) {
		super.afterJob(jobExecution);

		logger.info("end job: {} {}", jobExecution.getJobId().toString(), jobExecution.getJobConfigurationName());
	}
}
