package com.kwbt.nk.analyzer.repo;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

@Repository
public class SQLiteRepo {

	@Autowired
	private JdbcTemplate jdbcTemplate;

	public <T> List<T> selectList(String sql, Class<T> cls) {

		// でかいSelectと分かっているので、マジックナンバーで記載。
		jdbcTemplate.setFetchSize(1000);

		RowMapper<T> rowMapper = new BeanPropertyRowMapper<>(cls);

		return jdbcTemplate.query(sql, rowMapper);

	}

	public Integer selectInt(String sql) {

		return jdbcTemplate.queryForObject(sql, Integer.class);
	}
}
