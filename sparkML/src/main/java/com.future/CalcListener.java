package com.future;// Generated from Calc.g4 by ANTLR 4.8
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link CalcParser}.
 */
public interface CalcListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link CalcParser#prog}.
	 * @param ctx the parse tree
	 */
	void enterProg(CalcParser.ProgContext ctx);
	/**
	 * Exit a parse tree produced by {@link CalcParser#prog}.
	 * @param ctx the parse tree
	 */
	void exitProg(CalcParser.ProgContext ctx);
	/**
	 * Enter a parse tree produced by the {@code cal_stat}
	 * labeled alternative in {@link CalcParser#stat}.
	 * @param ctx the parse tree
	 */
	void enterCal_stat(CalcParser.Cal_statContext ctx);
	/**
	 * Exit a parse tree produced by the {@code cal_stat}
	 * labeled alternative in {@link CalcParser#stat}.
	 * @param ctx the parse tree
	 */
	void exitCal_stat(CalcParser.Cal_statContext ctx);
	/**
	 * Enter a parse tree produced by the {@code assign}
	 * labeled alternative in {@link CalcParser#stat}.
	 * @param ctx the parse tree
	 */
	void enterAssign(CalcParser.AssignContext ctx);
	/**
	 * Exit a parse tree produced by the {@code assign}
	 * labeled alternative in {@link CalcParser#stat}.
	 * @param ctx the parse tree
	 */
	void exitAssign(CalcParser.AssignContext ctx);
	/**
	 * Enter a parse tree produced by the {@code print}
	 * labeled alternative in {@link CalcParser#stat}.
	 * @param ctx the parse tree
	 */
	void enterPrint(CalcParser.PrintContext ctx);
	/**
	 * Exit a parse tree produced by the {@code print}
	 * labeled alternative in {@link CalcParser#stat}.
	 * @param ctx the parse tree
	 */
	void exitPrint(CalcParser.PrintContext ctx);
	/**
	 * Enter a parse tree produced by the {@code number}
	 * labeled alternative in {@link CalcParser#expr}.
	 * @param ctx the parse tree
	 */
	void enterNumber(CalcParser.NumberContext ctx);
	/**
	 * Exit a parse tree produced by the {@code number}
	 * labeled alternative in {@link CalcParser#expr}.
	 * @param ctx the parse tree
	 */
	void exitNumber(CalcParser.NumberContext ctx);
	/**
	 * Enter a parse tree produced by the {@code parens}
	 * labeled alternative in {@link CalcParser#expr}.
	 * @param ctx the parse tree
	 */
	void enterParens(CalcParser.ParensContext ctx);
	/**
	 * Exit a parse tree produced by the {@code parens}
	 * labeled alternative in {@link CalcParser#expr}.
	 * @param ctx the parse tree
	 */
	void exitParens(CalcParser.ParensContext ctx);
	/**
	 * Enter a parse tree produced by the {@code as_expr}
	 * labeled alternative in {@link CalcParser#expr}.
	 * @param ctx the parse tree
	 */
	void enterAs_expr(CalcParser.As_exprContext ctx);
	/**
	 * Exit a parse tree produced by the {@code as_expr}
	 * labeled alternative in {@link CalcParser#expr}.
	 * @param ctx the parse tree
	 */
	void exitAs_expr(CalcParser.As_exprContext ctx);
	/**
	 * Enter a parse tree produced by the {@code id}
	 * labeled alternative in {@link CalcParser#expr}.
	 * @param ctx the parse tree
	 */
	void enterId(CalcParser.IdContext ctx);
	/**
	 * Exit a parse tree produced by the {@code id}
	 * labeled alternative in {@link CalcParser#expr}.
	 * @param ctx the parse tree
	 */
	void exitId(CalcParser.IdContext ctx);
	/**
	 * Enter a parse tree produced by the {@code md_expr}
	 * labeled alternative in {@link CalcParser#expr}.
	 * @param ctx the parse tree
	 */
	void enterMd_expr(CalcParser.Md_exprContext ctx);
	/**
	 * Exit a parse tree produced by the {@code md_expr}
	 * labeled alternative in {@link CalcParser#expr}.
	 * @param ctx the parse tree
	 */
	void exitMd_expr(CalcParser.Md_exprContext ctx);
	/**
	 * Enter a parse tree produced by the {@code pow_expr}
	 * labeled alternative in {@link CalcParser#expr}.
	 * @param ctx the parse tree
	 */
	void enterPow_expr(CalcParser.Pow_exprContext ctx);
	/**
	 * Exit a parse tree produced by the {@code pow_expr}
	 * labeled alternative in {@link CalcParser#expr}.
	 * @param ctx the parse tree
	 */
	void exitPow_expr(CalcParser.Pow_exprContext ctx);
}
