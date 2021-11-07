package com.kwbt.nk.analyzer.step;

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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kwbt.nk.analyzer.repo.SQLiteRepo;
import com.kwbt.nk.analyzer.util.MyFileUtil;
import com.kwbt.nk.common.Parser;
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

        ExecutionContext context = chunkContext
                .getStepContext()
                .getStepExecution()
                .getExecutionContext();

        @SuppressWarnings("unchecked")
        List<Parser> list = (List<Parser>) context.get(A02_MultiSelectPartitioner.SPLITED_LIST_KEY);

        run(list);

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
}
