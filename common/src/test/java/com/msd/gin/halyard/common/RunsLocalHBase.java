package com.msd.gin.halyard.common;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.*;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

@Retention(RUNTIME)
@Target(TYPE)
@EnabledOnOs(OS.LINUX)
public @interface RunsLocalHBase {

}
