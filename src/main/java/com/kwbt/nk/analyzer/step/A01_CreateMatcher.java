package com.kwbt.nk.analyzer.step;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kwbt.nk.analyzer.repo.SQLiteRepo;
import com.kwbt.nk.analyzer.util.MyFileUtil;
import com.kwbt.nk.analyzer.util.SQLBuilder;
import com.kwbt.nk.common.FeatureMatcher;
import com.kwbt.nk.common.MatcherConst;
import com.kwbt.nk.common.Parser;
import com.kwbt.nk.common.Range;
import com.kwbt.nk.db.entity.Feature;

/**
 * Featureテーブルから全件データを取得し、FeatureMatcherクラスを作成
 */
@Component
public class A01_CreateMatcher implements Tasklet {

    private final static Logger logger = LoggerFactory.getLogger(A01_CreateMatcher.class);

    /************************************************
     * ymlファイルのProperty値
     */
    @Value("${nk.file.creatematcher:tmp-creatematcher.json}")
    private String outputFileName;

    public void callProperties() {
        logger.info(String.format("property get: outputFileName       : %s", outputFileName));
    }

    /************************************************
     * DIインスタンス
     */
    @Autowired
    private SQLiteRepo repo;

    @Autowired
    private MyFileUtil fileUtil;

    private final SQLBuilder sqlBuilder = new SQLBuilder();

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

        // DBからfeatureテーブルを全件取得
        logger.info("start select");
        final List<Feature> featureList = repo.selectList("select * from feature;", Feature.class);
        logger.info("select list size is {}", featureList.size());

        List<FeatureMatcher> originalMatcherList = createMatcher(featureList);
        logger.info("feature matcher list size is {}", originalMatcherList.size());

        deleteDuplicateReNew(originalMatcherList);

        logger.info("feature matcher removed duplicated. after size is {}", originalMatcherList.size());

        sortCount(originalMatcherList);

        ObjectMapper mapper = new ObjectMapper();
        final List<String> parserList = new ArrayList<>(originalMatcherList.size());
        for (FeatureMatcher e : originalMatcherList) {
            Parser parser = new Parser();
            parser.featureMatcher = e;
            parser.sql = sqlBuilder.createSQLAllNum(parser.featureMatcher);
            parserList.add(mapper.writeValueAsString(parser));
        }

        // 次ジョブへ渡すデータをファイルに出力する。
        fileUtil.writeFile(outputFileName, parserList);
        logger.info("result wrote " + outputFileName);
        return RepeatStatus.FINISHED;
    }

    /**
     * マッチャー枠を生成する。
     *
     * @param list
     * @return
     */
    private List<FeatureMatcher> createMatcher(List<Feature> list) {

        List<FeatureMatcher> result = new ArrayList<>(list.size());
        for (Feature e : list) {
            try {
                FeatureMatcher matcher = new FeatureMatcher();
                matcher.count = 1;
                matcher.age = getRangeDouble(e.getAge(), MatcherConst.ageMap);
                matcher.course = getRangeStr(e.getCourse(), MatcherConst.courseMap);
                matcher.dhweight = getRangeDouble(e.getDhweight(), MatcherConst.dhweightMap);
                matcher.distance = getRangeDouble(e.getDistance(), MatcherConst.distanceMap);
                matcher.dsl = getRangeDouble(e.getDsl(), MatcherConst.dslMap);
                matcher.hweight = getRangeDouble(e.getHweight(), MatcherConst.hweightMap);
                matcher.odds = getRangeDouble(e.getOdds(), MatcherConst.oddsMap);
                matcher.sex = getRangeStr(e.getSex(), MatcherConst.sexMap);
                matcher.surface = getRangeStr(e.getSurface(), MatcherConst.surfaceMap);
                matcher.weather = getRangeStr(e.getWeather(), MatcherConst.weatherMap);
                result.add(matcher);
            } catch (Exception ex) {
                logger.error("causes : {}", e);
                throw ex;
            }
        }
        return result;
    }

    private int getRangeStr(String i, Map<Integer, String> map) {

        if (map.isEmpty()) {
            return MatcherConst.VALUE_NONE;
        }

        if (StringUtils.isBlank(i)) {
            return MatcherConst.VALUE_NONE;
        }

        int result = MatcherConst.VALUE_INVALID;
        for (Integer key : map.keySet()) {
            if (map.get(key).equals(i)) {
                result = key;
                break;
            }
        }
        return result;
    }

    private int getRangeDouble(Double i, Map<Integer, Range<Double>> map) {

        if (map.isEmpty()) {
            return MatcherConst.VALUE_NONE;
        }

        if (i == null) {
            return MatcherConst.VALUE_NONE;
        }

        int result = MatcherConst.VALUE_INVALID;
        for (Integer key : map.keySet()) {
            Range<Double> r = map.get(key);
            if ((r == null && i == null)
                    || (r != null && r.isContain(i))) {
                result = key;
                break;
            }
        }
        return result;
    }

    /**
     * リストの重複を削除し、重複した数をカウントする。<br>
     *
     * @param list
     */
    private void deleteDuplicateReNew(List<FeatureMatcher> matcherList) {

        // summingIntの1で、カウント変数が初期値1であるのを保証している。
        logger.info("start remove duplicate by stream");
        Map<FeatureMatcher, Integer> asdf = matcherList
                .stream()
                .collect(
                        Collectors.groupingBy(
                                Function.identity(),
                                Collectors.summingInt(s -> 1)));
        logger.info("finish remove duplicate by stream");

        logger.info("start map to list");
        final List<FeatureMatcher> tmp = new ArrayList<>(asdf.size());
        for (FeatureMatcher fm : asdf.keySet()) {

            Integer count = asdf.get(fm);
            fm.count = count;
            tmp.add(fm);
        }
        logger.info("finished map to list");

        matcherList.clear();
        matcherList.addAll(tmp);
    }

    /**
     * FeatureMatcherクラス内のcount変数で降順ソートする
     *
     * @param list
     */
    private void sortCount(List<FeatureMatcher> list) {

        list.sort(Comparator.comparing(FeatureMatcher::getCount).reversed());
    }

}
