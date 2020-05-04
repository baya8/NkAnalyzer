package com.kwbt.nk.analyzer.step;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kwbt.nk.analyzer.util.MyFileUtil;
import com.kwbt.nk.common.Parser;

/**
 * マルチスレッドで稼働させるタスクレットの処理元データ分割を行う。
 *
 * @author baya
 */
@Component
public class A02_MultiSelectPartitioner implements Partitioner {

    private final static Logger logger = LoggerFactory.getLogger(A02_MultiSelectPartitioner.class);

    public final static String SPLITED_LIST_KEY = "SPLITED_LIST_KEY";

    /************************************************
     * ymlファイルのProperty値
     */
    @Value("${nk.file.creatematcher:tmp-creatematcher.json}")
    private String inputFileName;

    /**
     * SQLへの並列分散実行をデバッグ用の少ない数にする場合はtrueにする。
     */
    @Value("${nk.calculation-debug-mode:false}")
    public Boolean calcDebugMode;

    public void callProperties() {
        logger.info(String.format("property get: inputFileName        : %s", inputFileName));
        logger.info(String.format("property get: calcDebugMode        : %s", calcDebugMode));
    }

    /************************************************
     * DIインスタンス
     */
    @Autowired
    private MyFileUtil fileUtil;

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {

        logger.info("partition start");

        Map<String, ExecutionContext> map = new HashMap<>();
        try {
            List<Parser> readList = readList();

            // デバッグ用の少ない数で実行するフラグがONの場合は、1000件上限とする。
            if (calcDebugMode) {
                readList = readList.subList(0, 1000);
            }

            int num = readList.size() / gridSize + 1;

            // subListメソッドでListを分割する際、
            // endとする値は、切りたいインデックス+ 1となる
            // つまりendと次のstartの値は同じになる。
            int start = 0;
            int end = num;

            for (int i = 0; i < gridSize; i++) {

                ExecutionContext context = new ExecutionContext();

                // endインデックス値がリストのサイズを超えた場合
                // または、周回数が最終週に達した場合
                // リストの最後のインデックスをendとする。
                if (end > readList.size() || (i + 1) == gridSize) {
                    end = readList.size() - 1;
                }

                List<Parser> splitedList = new ArrayList<>(readList.subList(start, end));
                context.put(SPLITED_LIST_KEY, splitedList);

                map.put("partition" + i, context);

                logger.info("split index from " + start + " to " + end);

                start = end;
                end = end + num;
            }

        } catch (Exception e) {

            logger.info(inputFileName + "の読み込みに失敗");
            e.printStackTrace();
            map.clear();
        }

        logger.info("partition end");

        return map;
    }

    private List<Parser> readList() throws FileNotFoundException, IOException {

        logger.info("readList start");

        List<String> readedList = fileUtil.readFile(inputFileName);
        ObjectMapper mapper = new ObjectMapper();
        List<Parser> result = new ArrayList<>(readedList.size());
        for (String s : readedList) {
            result.add(mapper.readValue(s, Parser.class));
        }

        logger.info("readList end");
        return result;
    }

}
