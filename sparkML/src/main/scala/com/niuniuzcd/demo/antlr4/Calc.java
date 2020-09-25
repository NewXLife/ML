package com.niuniuzcd.demo.antlr4;


import com.future.CalcBaseVisitor;
import com.future.CalcParser;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;


/**
 * @author zcd
 * @date 2019-07-30 15:05
 */
public class Calc extends CalcBaseVisitor<Double>
{
    Map<String, Double> memory = new HashMap<String, Double>();

    // ID '=' expr ';'
    @Override
    public Double visitAssign(CalcParser.AssignContext ctx)
    {
        String id = ctx.ID().getText();
        Double value = visit(ctx.expr());
        memory.put(id, value);
        return value;
    }

    // expr ';'
    @Override
    public Double visitCal_stat(CalcParser.Cal_statContext ctx)
    {
        Double value = visit(ctx.expr());
        return value;
    }

    // 'print' '(' expr ')' ';'
    @Override
    public Double visitPrint(CalcParser.PrintContext ctx)
    {
        Double value = visit(ctx.expr());
        DecimalFormat df = new DecimalFormat("#.###");
        System.out.println(df.format(value));
        return value;
    }

    // <assco=right>expr POW expr
    @Override
    public Double visitPow_expr(CalcParser.Pow_exprContext ctx)
    {
        Double truth = visit(ctx.expr(0));
        Double power = visit(ctx.expr(1));
        return Math.pow(truth, power);
    }


    // expr op=(MUL|DIV) expr
    @Override
    public Double visitMd_expr(CalcParser.Md_exprContext ctx)
    {
        Double left = visit(ctx.expr(0));
        Double right = visit(ctx.expr(1));
        if (ctx.op.getType() == CalcParser.MUL)
        {
            return left * right;
        } else if (ctx.op.getType() == CalcParser.DIV)
        {
            if (new BigDecimal(right).compareTo(new BigDecimal(0.0)) == 0)
            {
                return 0.0;
            }
            return left / right;
        } else
        {
            int left_int = (new Double(left)).intValue();
            int right_int = (new Double(right).intValue());
            if (new BigDecimal(right_int).compareTo(new BigDecimal(right)) == 0
                    && new BigDecimal(left_int).compareTo(new BigDecimal(left_int)) == 0)
            {
                return Double.valueOf(left_int % right_int);
            }
            System.out.println("Mod\'%\' operations should be integer.");
            return 0.0;
        }
    }

    // expr op=(ADD|SUB) expr
    @Override
    public Double visitAs_expr(CalcParser.As_exprContext ctx)
    {
        Double left = visit(ctx.expr(0));
        Double right = visit(ctx.expr(1));
        if (ctx.op.getType() == CalcParser.ADD)
        {
            return left + right;
        } else
        {
            return left - right;
        }
    }

    // sign=(ADD|SUB)? NUM
    @Override
    public Double visitNumber(CalcParser.NumberContext ctx)
    {
        int child = ctx.getChildCount();
        Double value = Double.valueOf(ctx.NUM().getText());
        if (child == 2 && ctx.sign.getType() == CalcParser.SUB)
        {
            return 0 - value;
        }
        return value;
    }

    // ID
    @Override
    public Double visitId(CalcParser.IdContext ctx)
    {
        String id = ctx.ID().getText();
        if (memory.containsKey(id))
        {
            return memory.get(id);
        }
        System.out.println("undefined identifier \"" + id + "\".");
        return 0.0;
    }

    //'(' expr ')'
    @Override
    public Double visitParens(CalcParser.ParensContext ctx)
    {
        Double value = visit(ctx.expr());
        return value;
    }
}
