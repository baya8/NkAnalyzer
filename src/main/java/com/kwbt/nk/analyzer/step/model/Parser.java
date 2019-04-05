package com.kwbt.nk.analyzer.step.model;

import java.io.Serializable;
import java.util.List;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.kwbt.nk.common.FeatureMatcher;
import com.kwbt.nk.db.entity.FeaturePayoff;

/**
 * Tasklet間の受け渡し用モデル
 *
 * @author baya
 */
public class Parser implements Serializable {

	/**
	 * 検索条件が1つ単位で格納される
	 */
	public FeatureMatcher featureMatcher;

	/**
	 * 検索条件に対する、検索結果が格納される。
	 */
	public List<FeaturePayoff> featurePayoffList;

	/**
	 * featureMatcherから生成されたSQL文が格納される。
	 */
	public String sql;


	@Override
	public int hashCode() {
		return HashCodeBuilder.reflectionHashCode(this);
	}

	@Override
	public boolean equals(Object obj) {
		return EqualsBuilder.reflectionEquals(this, obj);
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this, ToStringStyle.JSON_STYLE);
	}
}
