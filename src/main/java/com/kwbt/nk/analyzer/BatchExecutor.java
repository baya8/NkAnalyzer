package com.kwbt.nk.analyzer;

import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import com.kwbt.nk.analyzer.parts.JobListener;
import com.kwbt.nk.analyzer.step.CreateMatcher;
import com.kwbt.nk.analyzer.step.MultiSelectPartitioner;
import com.kwbt.nk.analyzer.step.MultiSelectSlave;
import com.kwbt.nk.analyzer.step.ReadAndCalc;
import com.kwbt.nk.analyzer.util.MyFileUtil;

/**
 * 大元クラス。<br>
 *
 * @author baya
 */
@Configuration
@EnableBatchProcessing
public class BatchExecutor {

	private final static Logger logger = LoggerFactory.getLogger(BatchExecutor.class);

	/************************************************
	 * ymlファイルのProperty値
	 */
	@Value("${spring.datasource.driver-class-name}")
	private String driverName;

	@Value("${spring.datasource.url}")
	private String url;

	@Value("${nk.gridsize:3}")
	private Integer gridSize;

	@Value("${nk.analyze:''}")
	private String analyzeDate;

	private final static int dateLength = 14;

	@Value("${nk.task.createMatcher:false}")
	private Boolean execTaskCreateMatcher;

	@Value("${nk.task.calculation:false}")
	private Boolean execTaskCalculation;

	@Value("${nk.task.readAndCalc:false}")
	private Boolean execTaskReadAndCalc;

	/**
	 * Property値をバッチ起動時にログ出力
	 */
	public void callProperties() {
		logger.info(String.format("property get: driverName           : %s", driverName));
		logger.info(String.format("property get: url                  : %s", url));
		logger.info(String.format("property get: gridSize             : %s", gridSize));
		logger.info(String.format("property get: analyzeDate          : %s", analyzeDate));
		logger.info(String.format("property get: execTaskCreateMatcher: %s", execTaskCreateMatcher));
		logger.info(String.format("property get: execTaskCalculation  : %s", execTaskCalculation));
		logger.info(String.format("property get: execTaskReadAndCalc  : %s", execTaskReadAndCalc));
	}

	/************************************************
	 * DIインスタンス
	 */
	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private CreateMatcher createMatcher;

	@Autowired
	private MultiSelectSlave multiSelectSlave;

	@Autowired
	private MultiSelectPartitioner myPartitioner;

	@Autowired
	private ReadAndCalc readAndCalc;

	@Bean
	public Job initialAndTaskletDefine() {

		this.callProperties();
		myPartitioner.callProperties();
		createMatcher.callProperties();
		multiSelectSlave.callProperties();
		readAndCalc.callProperties();

		logger.info("start analyze new data");

		if (StringUtils.isNotBlank(analyzeDate)) {

			logger.info("start analyze InComplite data at {}", analyzeDate);

			// 指定の日付に対する処理を再開
			// SQL発行後、ファイルを結合する際の計算でエラーがあったため、その対応
			if (analyzeDate.length() != dateLength) {
				throw new RuntimeException("日付処理に必要な値の桁数が" + dateLength + "ではありません");
			}

			// 作業ディレクトリを、ymlファイルの指定値に設定する。
			LocalDateTime requiredWorkDirTime = LocalDateTime.parse(
					analyzeDate,
					DateTimeFormatter.ofPattern(
							MyFileUtil.localDateFormatterYMD + MyFileUtil.localDateFormatterHMS));

			// 作業ディレクトリ
			File targetDir = new File(
					MyFileUtil.getResutlWorkDirPath(requiredWorkDirTime));

			if (!targetDir.exists()) {
				throw new RuntimeException("対象の作業フォルダが存在しません。 ：" + targetDir.getAbsolutePath());
			}

			// 作業ディレクトリをMyFileUtilsへ設定
			MyFileUtil.workDir = new File(
					MyFileUtil.getResutlWorkDirPath(requiredWorkDirTime))
							.getAbsolutePath();

			return defineJob();

		} else {

			// 作業用ディレクトリを作成
			MyFileUtil.makeDir();

			// 新規解析開始
			return defineJob();
		}
	}

	/**
	 * 実行するジョブ、順番を定義<br>
	 * ymlファイル側で、どのタスクを実行するかを定義。
	 *
	 * @return
	 */
	private Job defineJob() {

		JobBuilder jb = jobBuilderFactory.get("job")
				.incrementer(new RunIdIncrementer())
				.listener(listener());

		boolean firstJob = true;

		SimpleJobBuilder jbb = null;

		if (execTaskCreateMatcher) {
			jbb = jb.start(createMatcherStep());
			firstJob = false;
		}

		if (execTaskCalculation) {

			if (firstJob) {
				jbb = jb.start(calcurationStep());
				firstJob = false;
			} else {
				jbb.next(calcurationStep());
			}
		}

		if (execTaskReadAndCalc) {

			if (firstJob) {
				jbb = jb.start(readAndCalcStep());
				firstJob = false;
			} else {
				jbb.next(readAndCalcStep());
			}
		}

		return jbb.build();
	}

	@Bean
	public JobExecutionListener listener() {
		return new JobListener();
	}

	@Bean
	public SimpleAsyncTaskExecutor taskExecutor() {
		return new SimpleAsyncTaskExecutor();
	}

	/**
	 * Step1:CreateMatcherステップを実行
	 *
	 * @return
	 */
	@Bean
	public Step createMatcherStep() {
		return stepBuilderFactory.get("createMatcher")
				.tasklet(createMatcher)
				.build();
	}

	/**
	 * Step2:Masterステップ<br>
	 * MultiSelectSlaveスレーブを多重起動
	 *
	 * @return
	 */
	@Bean
	public Step calcurationStep() {
		return stepBuilderFactory.get("master")
				.partitioner(slaveStep().getName(), myPartitioner)
				.partitionHandler(partitionHandler())
				.build();
	}

	/**
	 * MultiSelectSlaveスレーブ
	 *
	 * @return
	 */
	@Bean
	public Step slaveStep() {
		return stepBuilderFactory.get("multiSelectSlave")
				.tasklet(multiSelectSlave)
				.build();
	}

	@Bean
	public PartitionHandler partitionHandler() {

		// 多重実行時の多重度、ステップの設定などを行う。
		TaskExecutorPartitionHandler handler = new TaskExecutorPartitionHandler();
		handler.setGridSize(gridSize);
		handler.setTaskExecutor(taskExecutor());
		handler.setStep(slaveStep());

		logger.info("set multi-thread num: {}", gridSize);

		try {
			handler.afterPropertiesSet();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return handler;
	}

	/**
	 * 多重処理による複数ファイルを1つに纏め、諸計算を実行
	 */
	@Bean
	public Step readAndCalcStep() {
		return stepBuilderFactory.get("readAndCalc")
				.tasklet(readAndCalc)
				.build();
	}
}
