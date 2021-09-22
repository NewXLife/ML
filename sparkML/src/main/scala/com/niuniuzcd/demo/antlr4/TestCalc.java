package com.niuniuzcd.demo.antlr4;

import com.future.CalcLexer;
import com.future.CalcParser;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

/**
 * antlr4应用场景：
 *
 * 1.定制特定领域语言（DSL)
 * 类似hibernate中的HQL，用DSL来定义要执行操作的高层语法，这种语法接近人可理解的语言，由DSL到计算机语言的翻译则通过ANTLR来做，可在ANTLR的结构语言中定义DSL命令具体要执行何种操作。
 * 2.文本解析 可利用ANTLR解析JSON，HTML，XML，EDIFACT，或自定义的报文格式。解析出来的信息需要做什么处理也可以在结构文件中定义。
 * 3.数学计算 加减乘除，线性方程，几何运算，微积分等等
 *
 * @author zcd
 * @date 2019-07-30 15:36
 */
public class TestCalc
{
    /**
     * Listener 和 Visitor是两种遍历模式
     * 1、Listener (通过结点监听，触发处理方法)
     * 程序员不需要显示定义遍历语法树的顺序，实现简单
     * 缺点，不能显示控制遍历语法树的顺序
     * 动作代码与文法产生式解耦，利于文法产生式的重用
     * 没有返回值，需要使用map、栈等结构在节点间传值
     *
     * 2、Visitor (访问者模式，主动遍历)
     * 程序员可以显示定义遍历语法树的顺序
     * 不需要与antlr遍历类 ParseTreeWalker 一起使用，直接对tree操作
     * 动作代码与文法产生式解耦，利于文法产生式的重用
     * visitor方法可以直接返回值，返回值的类型必须一致，不需要使用map这种节点间传值方式，效率高
     * @param args
     */
    public static void main(String[] args)
    {

        /**
         * 一个语言的解析过程一般过程是 词法分析-->语法分析。
         * 这是ANTLR4为我们生成的框架代码，而我们唯一要做的是自己实现一个Vistor，一般从 ExprBaseVistor 继承即可
         */
        CharStream input = CharStreams.fromString("(((3+10-5)*2/5-100) + 10)*2-20 ;");

        //进行词法分析
        CalcLexer lexer = new CalcLexer(input);

        //将词法分析为词（token）
        CommonTokenStream tokens = new CommonTokenStream(lexer);

        //进行语法分析
        CalcParser parser = new CalcParser(tokens);

        //生成AST树（Abstract Syntax Tree）
        ParseTree tree = parser.prog();

//        Calc 类从ExprBaseVistor继承
        Calc calculator = new Calc();
        System.out.println(calculator.visit(tree));
    }
}
