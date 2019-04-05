package com.kwbt.nk.analyzer.parts;

import javax.annotation.PostConstruct;

import org.springframework.batch.core.configuration.BatchConfigurationException;
import org.springframework.batch.core.configuration.annotation.BatchConfigurer;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.explore.support.MapJobExplorerFactoryBean;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;

@Component
public class MyConfigure implements BatchConfigurer {

	private PlatformTransactionManager transactionManager;
	private JobRepository jobRepository;
	private JobLauncher jobLauncher;
	private JobExplorer jobExplorer;

	@PostConstruct
	public void initialize() {
		if (this.transactionManager == null) {
			this.transactionManager = new ResourcelessTransactionManager();
		}

		try {
			MapJobRepositoryFactoryBean repoFactory = new MapJobRepositoryFactoryBean(this.transactionManager);
			repoFactory.afterPropertiesSet();
			this.jobRepository = repoFactory.getObject();

			MapJobExplorerFactoryBean explFactory = new MapJobExplorerFactoryBean(repoFactory);
			explFactory.afterPropertiesSet();

			this.jobExplorer = explFactory.getObject();
			this.jobLauncher = createJobLauncher();
		} catch (Exception ex) {
			throw new BatchConfigurationException(ex);
		}

	}

	protected JobLauncher createJobLauncher() throws Exception {
		SimpleJobLauncher launcher = new SimpleJobLauncher();
		launcher.setJobRepository(jobRepository);
		launcher.afterPropertiesSet();
		return launcher;
	}

	@Override
	public JobRepository getJobRepository() throws Exception {
		return jobRepository;
	}

	@Override
	public PlatformTransactionManager getTransactionManager() throws Exception {
		return transactionManager;
	}

	@Override
	public JobLauncher getJobLauncher() throws Exception {
		return jobLauncher;
	}

	@Override
	public JobExplorer getJobExplorer() throws Exception {
		return jobExplorer;
	}
}
