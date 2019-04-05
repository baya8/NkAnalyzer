package com.kwbt.nk.common;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class MatcherConst {

	/**
	 * FeatureMatcherのキー
	 */
	public final static int VALUE_NONE = -9;
	public final static int VALUE_INVALID = -1;

	/***********************************************************************************************
	 * Map
	 ***********************************************************************************************/
	public static final Map<Integer, Range<Double>> ageMap = new HashMap<>();
	static {
		// put(0, new Range<>(2., 2.));
		// put(1, new Range<>(3., 3.));
		// put(2, new Range<>(4., 4.));
		// put(3, new Range<>(5., 5.));
		// put(4, new Range<>(6., 999.));
	};

	public static final Map<Integer, String> courseMap = new HashMap<>();
	static {
		AtomicInteger i = new AtomicInteger(0);
		courseMap.put(i.getAndIncrement(), "右");
		courseMap.put(i.getAndIncrement(), "左");
		courseMap.put(i.getAndIncrement(), "直線");
		courseMap.put(i.getAndIncrement(), "外");
		courseMap.put(i.getAndIncrement(), "他");
	};

	public static final Map<Integer, Range<Double>> distanceMap = new HashMap<>();
	static {
		AtomicInteger i = new AtomicInteger(0);
		distanceMap.put(i.getAndIncrement(), new Range<>(1000., 1200. - 1));
		distanceMap.put(i.getAndIncrement(), new Range<>(1200., 1400. - 1));
		distanceMap.put(i.getAndIncrement(), new Range<>(1400., 1600. - 1));
		distanceMap.put(i.getAndIncrement(), new Range<>(1600., 1800. - 1));
		distanceMap.put(i.getAndIncrement(), new Range<>(1800., 2000. - 1));
		distanceMap.put(i.getAndIncrement(), new Range<>(2000., 2200. - 1));
		distanceMap.put(i.getAndIncrement(), new Range<>(2200., 2400. - 1));
		distanceMap.put(i.getAndIncrement(), new Range<>(2400., 2600. - 1));
		distanceMap.put(i.getAndIncrement(), new Range<>(2600., 9999.));
	};

	public static final Map<Integer, Range<Double>> dhweightMap = new HashMap<>();
	static {
		AtomicInteger i = new AtomicInteger(0);
		dhweightMap.put(i.getAndIncrement(), new Range<>(-9999., -20. - 1));
		dhweightMap.put(i.getAndIncrement(), new Range<>(-20., -10. - 1));
		dhweightMap.put(i.getAndIncrement(), new Range<>(-10., -5. - 1));
		dhweightMap.put(i.getAndIncrement(), new Range<>(-5., 0. - 1));
		dhweightMap.put(i.getAndIncrement(), new Range<>(0., 5. - 1));
		dhweightMap.put(i.getAndIncrement(), new Range<>(5., 10. - 1));
		dhweightMap.put(i.getAndIncrement(), new Range<>(10., 20. - 1));
		dhweightMap.put(i.getAndIncrement(), new Range<>(20., 9999.));
	};

	public static final Map<Integer, Range<Double>> dslMap = new HashMap<>();
	static {
		AtomicInteger i = new AtomicInteger(0);
		Range<Double> nuller = null;
		dslMap.put(i.getAndIncrement(), nuller);
		dslMap.put(i.getAndIncrement(), new Range<>(5., 10.));
		dslMap.put(i.getAndIncrement(), new Range<>(11., 17.));
		dslMap.put(i.getAndIncrement(), new Range<>(18., 24.));
		dslMap.put(i.getAndIncrement(), new Range<>(25., 31.));
		dslMap.put(i.getAndIncrement(), new Range<>(32., 38.));
		dslMap.put(i.getAndIncrement(), new Range<>(39., 99999.));
	};

	public static final Map<Integer, Range<Double>> hweightMap = new HashMap<>();
	static {
		AtomicInteger i = new AtomicInteger(0);
		hweightMap.put(i.getAndIncrement(), new Range<>(0., 400. - 1));
		hweightMap.put(i.getAndIncrement(), new Range<>(400., 420. - 1));
		hweightMap.put(i.getAndIncrement(), new Range<>(420., 440. - 1));
		hweightMap.put(i.getAndIncrement(), new Range<>(440., 460. - 1));
		hweightMap.put(i.getAndIncrement(), new Range<>(460., 480. - 1));
		hweightMap.put(i.getAndIncrement(), new Range<>(480., 500. - 1));
		hweightMap.put(i.getAndIncrement(), new Range<>(500., 525. - 1));
		hweightMap.put(i.getAndIncrement(), new Range<>(525., 550. - 1));
		hweightMap.put(i.getAndIncrement(), new Range<>(550., 9999.));
	};

	public static final Map<Integer, Range<Double>> oddsMap = new HashMap<>();
	static {
		AtomicInteger i = new AtomicInteger(0);
		oddsMap.put(i.getAndIncrement(), new Range<>(.0, 3.0 - 0.1));
		oddsMap.put(i.getAndIncrement(), new Range<>(.0, 4.0 - 0.1));
		oddsMap.put(i.getAndIncrement(), new Range<>(.0, 5.0 - 0.1));
		oddsMap.put(i.getAndIncrement(), new Range<>(.0, 6.0 - 0.1));
		oddsMap.put(i.getAndIncrement(), new Range<>(.0, 7.0 - 0.1));
		oddsMap.put(i.getAndIncrement(), new Range<>(.0, 8.0 - 0.1));
		oddsMap.put(i.getAndIncrement(), new Range<>(.0, 9.0 - 0.1));
		oddsMap.put(i.getAndIncrement(), new Range<>(.0, 10.0 - 0.1));
		oddsMap.put(i.getAndIncrement(), new Range<>(.0, 15.0 - 0.1));
		oddsMap.put(i.getAndIncrement(), new Range<>(.0, 20.0 - 0.1));
		oddsMap.put(i.getAndIncrement(), new Range<>(.0, 30.0 - 0.1));
		oddsMap.put(i.getAndIncrement(), new Range<>(.0, 40.0 - 0.1));
		oddsMap.put(i.getAndIncrement(), new Range<>(.0, 50.0 - 0.1));
		oddsMap.put(i.getAndIncrement(), new Range<>(.0, 70.0 - 0.1));
		oddsMap.put(i.getAndIncrement(), new Range<>(.0, 90.0 - 0.1));
		oddsMap.put(i.getAndIncrement(), new Range<>(.0, 9999.9));
	};

	public static final Map<Integer, String> sexMap = new HashMap<>();
	static {
		// sexMap.put(0, "牡");
		// sexMap.put(1, "牝");
		// sexMap.put(2, "セ");
	};

	public static final Map<Integer, String> surfaceMap = new HashMap<>();
	static {
		AtomicInteger i = new AtomicInteger(0);
		surfaceMap.put(i.getAndIncrement(), "芝");
		surfaceMap.put(i.getAndIncrement(), "ダ");
	};

	public static final Map<Integer, String> weatherMap = new HashMap<Integer, String>();
	static {
		AtomicInteger i = new AtomicInteger(0);
		weatherMap.put(i.getAndIncrement(), "晴");
		weatherMap.put(i.getAndIncrement(), "曇");
		weatherMap.put(i.getAndIncrement(), "雨");
		weatherMap.put(i.getAndIncrement(), "小雨");
		weatherMap.put(i.getAndIncrement(), "雪");
		weatherMap.put(i.getAndIncrement(), "他");
	};
}
