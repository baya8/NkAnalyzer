package com.kwbt.nk.analyzer.step.model;

import com.kwbt.nk.common.FeatureMatcher;
import com.kwbt.nk.common.Result;

public class JsonData {

	private Result resultModel;

	private FeatureMatcher featureMatcher;

	public Result getResultModel() {
		return resultModel;
	}

	public void setResultModel(Result resultModel) {
		this.resultModel = resultModel;
	}

	public FeatureMatcher getFeatureMatcher() {
		return featureMatcher;
	}

	public void setFeatureMatcher(FeatureMatcher featureMatcher) {
		this.featureMatcher = featureMatcher;
	}

}
