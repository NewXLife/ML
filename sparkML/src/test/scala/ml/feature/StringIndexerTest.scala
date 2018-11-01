package ml.feature

/**
  * StringIndexer 的作用是把元数据里的字符标签，按照出现的频次进行序列编码
  * 如果数据良好，如标签只有2中0，1格式就不需要了，对于多分类的问题或者是标签本身是字符串的编码方式如： “high”， ”low", "medium" 这个步骤就很有用
  * 转换格式后才能被spark处理
  *
  * IndexToString  是把之前序列化的编码转换为原始的方式， 回复之前可读性比较高的表示，这样在存储和模型显示结果上面，可读性比较高。
  */
object StringIndexerTest {

}
