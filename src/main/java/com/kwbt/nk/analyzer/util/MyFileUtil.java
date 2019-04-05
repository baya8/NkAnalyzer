package com.kwbt.nk.analyzer.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class MyFileUtil {

	private final static Logger logger = LoggerFactory.getLogger(MyFileUtil.class);

	public final static String localDateFormatterYMD = "uuuuMMdd";
	public final static String localDateFormatterHMS = "HHmmss";

	/**
	 * プログラム起動時で、作業用フォルダを作成する
	 */
	public static String workDir = new String();
	static {
		LocalDateTime now = LocalDateTime.now();
		String path = getResutlWorkDirPath(now);
		File workDirFile = new File(path);
		// workDirFile.mkdirs();
		MyFileUtil.workDir = workDirFile.getAbsolutePath();

		logger.info("work dir: {}", workDir);
	}

	/**
	 * 作業対象ディレクトリパスを、時間から取得
	 *
	 * @param time
	 * @return
	 */
	public static String getResutlWorkDirPath(LocalDateTime time) {
		return "result"
				+ File.separator
				+ time.format(DateTimeFormatter.ofPattern(localDateFormatterYMD))
				+ File.separator
				+ time.format(DateTimeFormatter.ofPattern(localDateFormatterHMS));
	}

	public static void makeDir() {
		new File(workDir).mkdirs();
	}

	/**
	 * 作業用フォルダのディレクトリを付与した状態でのファイル名を取得する
	 *
	 * @param fileName
	 * @return
	 */
	public String getFilePathWithWorkDir(String fileName) {
		return workDir + File.separator + fileName;
	}

	/**
	 * 指定の文字列リストをファイルに書き込む
	 *
	 * @param fileName
	 * @param list
	 * @throws IOException
	 */
	public void writeFile(String fileName, List<String> list) throws IOException {
		File output = new File(getFilePathWithWorkDir(fileName));
		output.createNewFile();

		try (FileWriter fw = new FileWriter(output);
				BufferedWriter bw = new BufferedWriter(fw);) {
			for (String e : list) {
				bw.write(e);
				bw.newLine();
			}
		}
	}
	// public <T> void writeFile(String fileName, List<T> list) throws IOException {
	//
	// File output = new File(getFilePathWithWorkDir(fileName));
	// output.createNewFile();
	//
	// try (FileWriter fw = new FileWriter(output);
	// BufferedWriter bw = new BufferedWriter(fw);) {
	//
	// for (T e : list) {
	// bw.write(e.toString());
	// bw.newLine();
	// }
	// bw.flush();
	// }
	// }

	/**
	 * 指定の文字列をファイルに書き込む
	 *
	 * @param fileName
	 * @param obj
	 * @throws IOException
	 */
	public void writeFile(String fileName, String obj) throws IOException {

		File output = new File(getFilePathWithWorkDir(fileName));
		output.createNewFile();

		try (FileWriter fw = new FileWriter(output);
				BufferedWriter bw = new BufferedWriter(fw);) {
			bw.write(obj);
			bw.newLine();
		}
	}

	/**
	 * アプリ起動時に作成される作業フォルダ内から、ファイルを読み込む
	 *
	 * @param fileName
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public List<String> readFile(String fileName) throws FileNotFoundException, IOException {
		return readFile(new File(getFilePathWithWorkDir(fileName)));
	}

	public List<String> readFile(File f) throws FileNotFoundException, IOException {

		List<String> pList = new ArrayList<>(20000);
		try (FileReader fs = new FileReader(f);
				BufferedReader br = new BufferedReader(fs);) {

			String line = null;
			while ((line = br.readLine()) != null) {
				pList.add(line);
			}
		}

		return pList;
	}
}
