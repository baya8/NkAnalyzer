package com.kwbt.nk.analyzer.step;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kwbt.nk.analyzer.repo.SQLiteRepo;
import com.kwbt.nk.analyzer.step.model.JsonData;
import com.kwbt.nk.analyzer.step.model.Parser;
import com.kwbt.nk.analyzer.util.MyFileUtil;
import com.kwbt.nk.analyzer.util.SQLBuilder;
import com.kwbt.nk.common.FeatureMatcher;
import com.kwbt.nk.common.Result;
import com.kwbt.nk.db.entity.FeaturePayoff;

/**
 * マルチスレッドで作成されたそれぞれのファイルを1つのファイルに結合する。
 *
 * @author baya
 */
@Component
public class ReadAndCalc implements Tasklet {

	private final static Logger logger = LoggerFactory.getLogger(ReadAndCalc.class);

	private final ObjectMapper mapper = new ObjectMapper();

	/************************************************
	 * ymlファイルのProperty値
	 */
	@Value("${nk.file.multiselect:tmp-multiselect.json}")
	private String inputFileName;

	@Value("${nk.file.concated:datasource.json}")
	private String outputFileName;

	@Value("${nk.order-of-finish-top:1}")
	private Integer orderOfFinishTop;

	public void callProperties() {
		logger.info(String.format("property get: inputFileName        : %s", inputFileName));
		logger.info(String.format("property get: outputFileName       : %s", outputFileName));
		logger.info(String.format("property get: orderOfFinishTop     : %s", orderOfFinishTop));
	}

	/************************************************
	 * DIインスタンス
	 */
	@Autowired
	private MyFileUtil fileUtil;

	@Autowired
	private SQLiteRepo sqliteRepo;

	@Override
	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

		try {

			// マルチスレッドで作成されたファイルリスト
			Set<File> inputFile = getFile();

			// 対象ファイルが存在しない場合は、例外を投げて処理を中断
			if (inputFile.size() == 0) {
				throw new FileNotFoundException(String.format("not input file: %s", inputFileName));
			}

			// マルチスレッドで作成されたファイルの内容をParserクラスへ変換
			List<Parser> concatedList = read(inputFile);

			List<JsonData> calcedList = calcuration(concatedList);

			Map<Integer, List<JsonData>> groupedList = separator(calcedList);

			// 1ファイルへ出力
			fileUtil.writeFile(outputFileName, mapper.writeValueAsString(groupedList));

		} catch (Exception e) {
			contribution.setExitStatus(ExitStatus.FAILED);
			throw e;
		}

		return RepeatStatus.FINISHED;
	}

	private Set<File> getFile() {

		final Set<File> result = new TreeSet<>();
		for (File f : new File(MyFileUtil.workDir).listFiles()) {
			if (f.getName().contains(inputFileName)) {
				result.add(f);
			}
		}

		return result;
	}

	/**
	 * マルチスレーブステップで作成されたファイルを読み込み、指定の型へ変換。
	 *
	 * @param files
	 * @return
	 * @throws Exception
	 */
	private List<Parser> read(Set<File> files) throws Exception {

		final List<Parser> result = new ArrayList<>();
		for (File f : files) {

			final List<String> strList = fileUtil.readFile(f);

			for (String str : strList) {
				result.add(mapper.readValue(str, Parser.class));
			}
		}
		return result;
	}

	/**
	 * ペイオフの計算を行う。
	 *
	 * @param pList
	 * @return
	 */
	private List<JsonData> calcuration(List<Parser> pList) {

		final SQLBuilder sqlBuilder = new SQLBuilder();

		final List<JsonData> result = new ArrayList<>(pList.size());

		Integer skipElementCount = 0;
		for (Parser p : pList) {

			try {
				final FeatureMatcher matcher = p.featureMatcher;

				final List<FeaturePayoff> boughtPaper = p.featurePayoffList;

				// 馬券購入数
				final int bakenkonyu = boughtPaper.size();

				// 1着の馬がいるか探す
				final Set<Integer> uniqueRaceId = new HashSet<>();
				{
					for (FeaturePayoff f : boughtPaper) {

						// 1着馬のレースIDだけ抽出
						if (orderOfFinishTop.equals(f.getOrder_of_finish())) {
							uniqueRaceId.add(f.getRace_id());
						}
					}
				}

				if (!uniqueRaceId.isEmpty()) {
					// 1着馬のペイオフを取得
					String sql = sqlBuilder.createSQLSelectPayoffOrderOfFinishIsZero(uniqueRaceId);
					// logger.info("sql : {}", sql);
					int payoffSum = sqliteRepo.selectInt(sql);

					Result e = new Result();
					BeanUtils.copyProperties(matcher, e);
					e.bakenAllNum = bakenkonyu;
					e.bakenNumOfFinishedOne = uniqueRaceId.size();
					e.payoffSum = payoffSum;
					e.kaisyu = (payoffSum) / (bakenkonyu * 100.0) * 100.0;
					e.syoritsu = (uniqueRaceId.size()) / bakenkonyu * 100.0;

					JsonData oneLine = new JsonData();
					oneLine.setResultModel(e);
					oneLine.setFeatureMatcher(p.featureMatcher);
					result.add(oneLine);

				} else {
					skipElementCount++;
				}
			} catch (Exception e) {
				logger.error("エラー:parser sql {}", p.sql);
				throw e;
			}
		}

		logger.info("skipped count: {}", skipElementCount);

		return result;
	}

	// private List<Result> calc(List<Parser> pList) {
	//
	// final SQLBuilder sqlBuilder = new SQLBuilder();
	//
	// final List<Result> result = new ArrayList<>(pList.size());
	//
	// Integer skipElementCount = 0;
	// for (Parser p : pList) {
	//
	// try {
	// final FeatureMatcher matcher = p.featureMatcher;
	//
	// final List<FeaturePayoff> boughtPaper = p.featurePayoffList;
	//
	// // 馬券購入数
	// final int bakenkonyu = boughtPaper.size();
	//
	// // 1着の馬がいるか探す
	// final Set<Integer> uniqueRaceId = new HashSet<>();
	// {
	// for (FeaturePayoff f : boughtPaper) {
	//
	// // 1着馬のレースIDだけ抽出
	// if (orderOfFinishTop.equals(f.getOrder_of_finish())) {
	// uniqueRaceId.add(f.getRace_id());
	// }
	// }
	// // if (matcher.count != uniqueRaceId.size()) {
	// // logger.error("データ整合性エラー:count is {}, unique size is {}, matcher is {}",
	// // matcher.count, uniqueRaceId.size(), matcher.toString());
	// // throw new IllegalArgumentException();
	// // }
	// }
	//
	// if (!uniqueRaceId.isEmpty()) {
	// // 1着馬のペイオフを取得
	// String sql = sqlBuilder.createSQLSelectPayoffOrderOfFinishIsZero(uniqueRaceId);
	// // logger.info("sql : {}", sql);
	// int payoffSum = sqliteRepo.selectInt(sql);
	//
	// Result e = new Result();
	// BeanUtils.copyProperties(matcher, e);
	// e.bakenAllNum = bakenkonyu;
	// e.bakenNumOfFinishedOne = uniqueRaceId.size();
	// e.payoffSum = payoffSum;
	// e.kaisyu = (payoffSum) / (bakenkonyu * 100.0) * 100.0;
	// e.syoritsu = (uniqueRaceId.size()) / bakenkonyu * 100.0;
	//
	// result.add(e);
	// } else {
	// skipElementCount++;
	// }
	//
	// } catch (Exception e) {
	// logger.error("エラー:parser sql {}", p.sql);
	// throw e;
	// }
	//
	// logger.info("skipped count: {}", skipElementCount);
	// }
	//
	// return result;
	// }

	/**
	 * レース距離ごとにグルーピングする。
	 *
	 * @param list
	 * @return
	 */
	private Map<Integer, List<JsonData>> separator(List<JsonData> list) {

		Map<Integer, List<JsonData>> map = new HashMap<>();
		for (JsonData r : list) {

			Integer distance = r.getFeatureMatcher().distance;
			List<JsonData> value = map.get(distance);
			if (CollectionUtils.isEmpty(value)) {
				map.put(distance, new ArrayList<>());
				value = map.get(distance);
			}
			value.add(r);
		}

		return map;
	}
}
