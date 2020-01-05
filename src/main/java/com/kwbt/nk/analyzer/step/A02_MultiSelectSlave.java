package com.kwbt.nk.analyzer.step;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteConfig.JournalMode;
import org.sqlite.SQLiteConfig.SynchronousMode;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kwbt.nk.analyzer.repo.SQLiteRepo;
import com.kwbt.nk.analyzer.step.model.Parser;
import com.kwbt.nk.analyzer.util.MyFileUtil;
import com.kwbt.nk.db.entity.FeaturePayoff;

/**
 * Parserクラス内にあるSQLを実行する<br>
 * マルチスレッドによる並列処理単位
 *
 * @author baya
 */
@Component
public class A02_MultiSelectSlave implements Tasklet {

    private final static Logger logger = LoggerFactory.getLogger(A02_MultiSelectSlave.class);

    /************************************************
     * ymlファイルのProperty値
     */
    @Value("${nk.file.multiselect:tmp-multiselect.json}")
    private String outputFileName;

    @Value("${nk.caluculation-logging-span:100}")
    private Integer loggingSpan;

    public void callProperties() {
        logger.info(String.format("property get: outputFileName       : %s", outputFileName));
        logger.info(String.format("property get: loggingSpan          : %s", loggingSpan));
    }

    /************************************************
     * DIインスタンス
     */
    @Autowired
    private MyFileUtil fileUtil;

    @Autowired
    private SQLiteRepo sqliteRepo;

    /**
     * Masterタスクから処理対象のパラメータを受け取って、 自分に与えられた処理を実行
     */
    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

        ExecutionContext context = chunkContext.getStepContext().getStepExecution().getExecutionContext();

        @SuppressWarnings("unchecked")
        List<Parser> list = (List<Parser>) context.get(A02_MultiSelectPartitioner.SPLITED_LIST_KEY);

        run(list);
        // run2(list);

        return RepeatStatus.FINISHED;
    }

    private void run(List<Parser> list) throws Exception {

        String threadID = String.valueOf(Thread.currentThread().getId());

        ObjectMapper mapper = new ObjectMapper();
        List<String> jsonList = new ArrayList<>(list.size());

        logger.info("start thread id: {}, list size is {}", threadID, list.size());

        try {

            for (int i = 0; i < list.size(); i++) {

                Parser p = list.get(i);
                p.featurePayoffList = sqliteRepo.selectList(p.sql, FeaturePayoff.class);
                jsonList.add(mapper.writeValueAsString(p));

                if (i % loggingSpan == 0) {
                    logger.info("【thread id {}】exec pass {}", threadID, i);
                }
            }

            logger.info("sql query is finished by {}", threadID);

        } catch (Exception e) {
            logger.info("unexpected error occured by {}", threadID);
            throw e;
        } finally {

            // 処理落ちしても現段階で処理終わったものは書き込みたい
            try {
                fileUtil.writeFile(outputFileName + threadID, jsonList);

            } catch (Exception e) {

                // ファイルの書き込みまで失敗したやつは知らん
                throw e;
            }
        }
    }

    private void run2(List<Parser> list) throws Exception {

        String threadID = String.valueOf(Thread.currentThread().getId());

        logger.info("start thread id: {}, list size is {}", threadID, list.size());

        SQLiteConfig config = new SQLiteConfig();
        config.setReadOnly(true);
        config.setJournalMode(JournalMode.MEMORY);
        config.setSynchronous(SynchronousMode.OFF);

        try (Connection con = DriverManager.getConnection("jdbc:sqlite:Q:\\masataka\\syuto\\DB\\1.4\\race.db",
                config.toProperties());) {

            long s1 = System.currentTimeMillis();
            for (int i = 0; i < list.size(); i++) {

                long s2 = System.currentTimeMillis();
                Parser p = list.get(i);
                // PreparedStatement pstmt = con.prepareStatement(p.sql).executeQuery();
                try (PreparedStatement pstmt = con.prepareStatement(p.sql);) {
                    pstmt.setFetchSize(10000);
                    ResultSet rs = pstmt.executeQuery();
                    logger.info("time ss: {}", System.currentTimeMillis() - s2);

                    long s3 = System.currentTimeMillis();
                    while (rs.next()) {
                        FeaturePayoff fp = new FeaturePayoff();
                        // fp.setRace_id(rs.getInt("race_id"));
                        // fp.setOrder_of_finish(rs.getInt("order_of_finish"));

                        if (CollectionUtils.isEmpty(p.featurePayoffList)) {
                            p.featurePayoffList = new ArrayList<>();
                        }
                        p.featurePayoffList.add(fp);
                    }
                    logger.info("time sss: {}", System.currentTimeMillis() - s3);
                }

                // logger.info("loop count: {}", p.featurePayoffList.size());

                if (i % 10 == 0) {
                    logger.info("【thread id {}】exec pass {}", threadID, i);
                }
            }

        } catch (Exception e) {
            throw e;
        }

        final ObjectMapper mapper = new ObjectMapper();

        mapper.writeValueAsString(list);
        fileUtil.writeFile(outputFileName + threadID, mapper.writeValueAsString(list));
    }
}
