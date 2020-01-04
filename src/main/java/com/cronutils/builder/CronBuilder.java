/*
 * Copyright 2015 jmrozanec
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cronutils.builder;

import com.cronutils.model.CompositeCron;
import com.cronutils.model.Cron;
import com.cronutils.model.SingleCron;
import com.cronutils.model.definition.CronDefinition;
import com.cronutils.model.field.CronField;
import com.cronutils.model.field.CronFieldName;
import com.cronutils.model.field.constraint.FieldConstraints;
import com.cronutils.model.field.definition.FieldDefinition;
import com.cronutils.model.field.expression.Always;
import com.cronutils.model.field.expression.FieldExpression;
import com.cronutils.model.field.expression.FieldExpressionFactory;
import com.cronutils.model.field.expression.visitor.ValidationFieldExpressionVisitor;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.utils.VisibleForTesting;

import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

import static com.cronutils.model.field.CronFieldName.*;
import static com.cronutils.utils.Preconditions.checkState;

public class CronBuilder {

    private final Map<CronFieldName, CronField> fields = new EnumMap<>(CronFieldName.class);
    private final CronDefinition definition;
    private ZonedDateTime start = null;
    private ZonedDateTime end = null;
    private final Set<Map<CronFieldName, CronField>> setOfFields = new LinkedHashSet<>();

    private CronBuilder(final CronDefinition definition) {
        this.definition = definition;
    }

    public static CronBuilder cron(final CronDefinition definition) {
        return new CronBuilder(definition);
    }

    public CronBuilder withDoY(final FieldExpression expression) {
        return addField(DAY_OF_YEAR, expression);
    }

    public CronBuilder withYear(final FieldExpression expression) {
        return addField(YEAR, expression);
    }

    public CronBuilder withDoM(final FieldExpression expression) {
        return addField(DAY_OF_MONTH, expression);
    }

    public CronBuilder withMonth(final FieldExpression expression) {
        return addField(MONTH, expression);
    }

    public CronBuilder withDoW(final FieldExpression expression) {
        return addField(DAY_OF_WEEK, expression);
    }

    public CronBuilder withHour(final FieldExpression expression) {
        return addField(HOUR, expression);
    }

    public CronBuilder withMinute(final FieldExpression expression) {
        return addField(MINUTE, expression);
    }

    public CronBuilder withSecond(final FieldExpression expression) {
        return addField(SECOND, expression);
    }

    public CronBuilder withStartAndEnd(final ZonedDateTime s, final ZonedDateTime t) {
        if (s.toInstant().compareTo(t.toInstant()) >= 0) {
            throw new IllegalArgumentException("start must be 'before' end time");
        }
        start = s;
        end = t;
        return this;
    }

    public Cron instance() {
        if (start != null) {
            split(start, end);
            return new CompositeCron(setOfFields.stream().map(f -> {
                SingleCron singleCron = new SingleCron(definition, new ArrayList<>(f.values()));
                System.out.println(singleCron.asString());
                return singleCron;
            }).collect(Collectors.toList()));
        } else {
            return new SingleCron(definition, new ArrayList<>(fields.values())).validate();
        }
    }

    private void split(ZonedDateTime start, ZonedDateTime end) {
        Cron primal = new SingleCron(definition, new ArrayList<>(fields.values())).validate();
        ExecutionTime primalExecuteTime = ExecutionTime.forCron(primal);

        if (start.toInstant().compareTo(end.toInstant()) >= 0) {
            return;
        }

        // ensure arguments
        Optional<ZonedDateTime> firstAfterStart = primalExecuteTime.nextExecution(start);
        if (!firstAfterStart.isPresent() || firstAfterStart.get().compareTo(end) > 0) {
            return;
        }
        Optional<ZonedDateTime> lastBeforeEnd = primalExecuteTime.lastExecution(end);
        if (!lastBeforeEnd.isPresent() || lastBeforeEnd.get().compareTo(start) < 0) {
            return;
        }

        // copy fields
        Map<CronFieldName, CronField> fields = new EnumMap<>(CronFieldName.class);
        copyTo(this.fields, fields);

        int startYear = start.getYear();
        int endYear = end.getYear();
        int yearsGap = endYear - startYear;
        if (yearsGap > 0) {
            if (primalExecuteTime.nextExecution(start.truncatedTo(ChronoUnit.DAYS).withDayOfYear(1))
                .get().compareTo(start) < 0) {
                split(start,
                    start.plus(1, ChronoUnit.YEARS).truncatedTo(ChronoUnit.DAYS).withDayOfYear(1)
                        .minusNanos(1));
                startYear++;
            }
            if (primalExecuteTime.lastExecution(
                end.plus(1, ChronoUnit.YEARS).truncatedTo(ChronoUnit.DAYS).withDayOfYear(1)
                    .minusNanos(1)).get().compareTo(end) > 0) {
                split(end.truncatedTo(ChronoUnit.DAYS).withDayOfYear(1), end);
                endYear--;
            }
            if (fillGap(fields, startYear, endYear, yearsGap, YEAR)) {
                return;
            }
            setAdd(fields);
            return;
        }
        addField(fields, YEAR, FieldExpressionFactory.on(start.getYear()));

        int startMonth = start.getMonth().getValue();
        int endMonth = end.getMonth().getValue();
        int monthGap = endMonth - startMonth;
        if (monthGap > 0) {
            if (primalExecuteTime
                .nextExecution(start.truncatedTo(ChronoUnit.DAYS).withDayOfMonth(1).minusNanos(1))
                .get().compareTo(start) < 0) {
                split(start,
                    start.plus(1, ChronoUnit.MONTHS).truncatedTo(ChronoUnit.DAYS).withDayOfMonth(1)
                        .minusNanos(1));
                startMonth++;
            }
            if (primalExecuteTime.lastExecution(
                end.plus(1, ChronoUnit.MONTHS).truncatedTo(ChronoUnit.DAYS).withDayOfMonth(1)
                    .minusNanos(1)).get().compareTo(end) > 0) {
                split(end.truncatedTo(ChronoUnit.DAYS).withDayOfMonth(1), end);
                endMonth--;
            }
            if (fillGap(fields, startMonth, endMonth, monthGap, MONTH)) {
                return;
            }
            setAdd(fields);
            return;
        }
        addField(fields, MONTH, FieldExpressionFactory.on(start.getMonth().getValue()));

        int startDayOfMonth = start.getDayOfMonth();
        int endDayOfMonth = end.getDayOfMonth();
        int daysGap = endDayOfMonth - startDayOfMonth;
        if (daysGap > 0) {
            if (primalExecuteTime
                .nextExecution(start.truncatedTo(ChronoUnit.DAYS).minusNanos(1))
                .get().compareTo(start) < 0) {
                split(start,
                    start.plus(1, ChronoUnit.DAYS).truncatedTo(ChronoUnit.DAYS).minusNanos(1));
                startDayOfMonth++;
            }
            if (primalExecuteTime.lastExecution(
                end.plus(1, ChronoUnit.DAYS).truncatedTo(ChronoUnit.DAYS)
                    .minusNanos(1)).get().compareTo(end) > 0) {
                split(end.truncatedTo(ChronoUnit.DAYS), end);
                endDayOfMonth--;
            }
            if (fillGap(fields, startDayOfMonth, endDayOfMonth, daysGap, DAY_OF_MONTH)) {
                return;
            }
            setAdd(fields);
            return;
        }
        addField(fields, DAY_OF_MONTH, FieldExpressionFactory.on(start.getDayOfMonth()));

        int startHour = start.getHour();
        int endHour = end.getHour();
        int hourGap = endHour - startHour;
        if (hourGap > 0) {
            if (primalExecuteTime
                .nextExecution(start.truncatedTo(ChronoUnit.HOURS).minusNanos(1))
                .get().compareTo(start) < 0) {
                split(start,
                    start.plus(1, ChronoUnit.HOURS).truncatedTo(ChronoUnit.HOURS).minusNanos(1));
                startHour++;
            }
            if (primalExecuteTime.lastExecution(
                end.plus(1, ChronoUnit.HOURS).truncatedTo(ChronoUnit.HOURS)
                    .minusNanos(1)).get().compareTo(end) > 0) {
                split(end.truncatedTo(ChronoUnit.HOURS), end);
                endHour--;
            }
            if (fillGap(fields, startHour, endHour, hourGap, HOUR)) {
                return;
            }
            setAdd(fields);

            return;
        }
        addField(fields, HOUR, FieldExpressionFactory.on(start.getHour()));

        int startMinute = start.getMinute();
        int endMinute = end.getMinute();
        int minuteGap = endMinute - startMinute;
        if (minuteGap > 0) {
            if (primalExecuteTime
                .nextExecution(start.truncatedTo(ChronoUnit.MINUTES).minusNanos(1))
                .get().compareTo(start) < 0) {
                split(start, start.plus(1, ChronoUnit.MINUTES).truncatedTo(ChronoUnit.MINUTES)
                    .minusNanos(1));
                startMinute++;
            }
            if (primalExecuteTime.lastExecution(
                end.plus(1, ChronoUnit.MINUTES).truncatedTo(ChronoUnit.MINUTES)
                    .minusNanos(1)).get().compareTo(end) > 0) {
                split(end.truncatedTo(ChronoUnit.MINUTES), end);
                endMinute--;
            }
            if (fillGap(fields, startMinute, endMinute, minuteGap, MINUTE)) {
                return;
            }
            setAdd(fields);
        }
        addField(fields, MINUTE, FieldExpressionFactory.on(start.getMinute()));
    }

    private boolean setAdd(Map<CronFieldName, CronField> fields) {
        return setOfFields.add(fields);
    }

    private boolean fillGap(Map<CronFieldName, CronField> fields, int start, int end, int gap,
        CronFieldName name) {
        if (gap > 1) {
            addField(fields, name, FieldExpressionFactory.between(start, end));
        } else if (gap > 0) {
            addField(fields, name, FieldExpressionFactory.on(start));
        } else {
            return true;
        }
        CronField field = fields.get(name);
        if ((field.getExpression() instanceof Always)) {
            return true;
        }
        return false;
    }

    private static void copyTo(Map<CronFieldName, CronField> src,
        Map<CronFieldName, CronField> dest) {
        src.forEach(dest::put);
    }

    @VisibleForTesting CronBuilder addField(final CronFieldName name,
        final FieldExpression expression) {
        checkState(definition != null, "CronBuilder not initialized.");

        final FieldConstraints constraints = definition.getFieldDefinition(name).getConstraints();
        expression.accept(new ValidationFieldExpressionVisitor(constraints));
        fields.put(name, new CronField(name, expression, constraints));

        return this;
    }

    void addField(Map<CronFieldName, CronField> fields, final CronFieldName name,
        final FieldExpression expression) {
        checkState(definition != null, "CronBuilder not initialized.");

        FieldDefinition definition = this.definition.getFieldDefinition(name);
        if (null == definition) {
            // definition do not support name.
            return;
        }
        final FieldConstraints constraints = definition.getConstraints();
        expression.accept(new ValidationFieldExpressionVisitor(constraints));
        fields.put(name, new CronField(name, expression, constraints));

    }
}
