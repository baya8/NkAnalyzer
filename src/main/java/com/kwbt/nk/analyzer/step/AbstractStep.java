package com.kwbt.nk.analyzer.step;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.beans.factory.annotation.Value;

public abstract class AbstractStep implements Tasklet {

    public void callProperties(Logger logger, Object instance, Class<?> cls) {

        List<Field> fields = Stream.of(cls.getDeclaredFields())
                .filter(e -> Objects.nonNull(e.getAnnotation(Value.class)))
                .collect(Collectors.toList());

        for (Field f : fields) {

            try {
                f.setAccessible(true);
                String name = f.getName();
                Object value = f.get(instance);
                logger.info(String.format("property get:%20s: %s", name, value));
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }
}
