package config_spider

import (
	"crawlab/constants"
	"crawlab/entity"
	"crawlab/model"
	"crawlab/utils"
	"errors"
	"fmt"
	"path/filepath"
)

type ScrapyGenerator struct {
	Spider     model.Spider
	ConfigData entity.ConfigSpiderData
}

// 生成爬虫文件
func (g ScrapyGenerator) Generate() error {
	// 生成 items.py
	if g.ConfigData.Goose {
		if err := g.ProcessGooseItems(); err != nil {
			return err
		}
	} else {
		if err := g.ProcessItems(); err != nil {
			return err
		}
	}
	if g.ConfigData.Proxy == "http" {
		// 修改settins.py 文件
		g.ProcessMiddlewaresSettings()
	}
	// 生成 spider.py
	if err := g.ProcessSpider(); err != nil {
		return err
	}
	return nil
}

// 生成 items.py
func (g ScrapyGenerator) ProcessItems() error {
	// 待处理文件名
	src := g.Spider.Src
	filePath := filepath.Join(src, "config_spider", "items.py")

	// 获取所有字段
	fields := g.GetAllFields()

	// 字段名列表（包含默认字段名）
	fieldNames := []string{
		"_id",
		"task_id",
		"ts",
	}

	// 加入字段
	for _, field := range fields {
		fieldNames = append(fieldNames, field.Name)
	}

	// 将字段名转化为python代码
	str := ""
	for _, fieldName := range fieldNames {
		line := g.PadCode(fmt.Sprintf("%s = scrapy.Field()", fieldName), 1)
		str += line
	}

	// 将占位符替换为代码
	if err := utils.SetFileVariable(filePath, constants.AnchorItems, str); err != nil {
		return err
	}

	return nil
}

// 生成 items.py
func (g ScrapyGenerator) ProcessMiddlewaresSettings() error {
	// 待处理文件名
	src := g.Spider.Src
	filePath := filepath.Join(src, "config_spider", "settings.py")
	str := ""
	str += g.PadCode(fmt.Sprintf("DOWNLOADER_MIDDLEWARES = {"), 0)
	str += g.PadCode(fmt.Sprintf("'config_spider.middlewares.ProxyMiddleware': 100,  "), 1)
	str += g.PadCode(fmt.Sprintf("}"), 0)
	// 将占位符替换为代码
	if err := utils.SetFileVariable(filePath, constants.AnchorProxy, str); err != nil {
		return err
	}

	return nil
}

func (g ScrapyGenerator) ProcessGooseItems() error {
	// 待处理文件名
	src := g.Spider.Src
	filePath := filepath.Join(src, "config_spider", "items.py")

	// 获取所有字段
	fields := g.GetAllFields()

	// 字段名列表（包含默认字段名）
	fieldNames := []string{
		"_id",
		"task_id",
		"ts",
		"title",
		"content",
		"raw_html",
		"publish_datetime_utc",
		"tags",
		"publish_date",
		"title_zh",
		"content_zh",
	}

	// 加入字段
	for _, field := range fields {
		fieldNames = append(fieldNames, field.Name)
	}
	fieldNames = removeDuplicateElement(fieldNames)

	// 将字段名转化为python代码
	str := ""
	for _, fieldName := range fieldNames {
		line := g.PadCode(fmt.Sprintf("%s = scrapy.Field()", fieldName), 1)
		str += line
	}

	// 将占位符替换为代码
	if err := utils.SetFileVariable(filePath, constants.AnchorItems, str); err != nil {
		return err
	}

	return nil
}

func removeDuplicateElement(items []string) []string {
	result := make([]string, 0, len(items))
	temp := map[string]struct{}{}
	for _, item := range items {
		if _, ok := temp[item]; !ok {
			temp[item] = struct{}{}
			result = append(result, item)
		}
	}
	return result
}

// 生成 spider.py
func (g ScrapyGenerator) ProcessSpider() error {
	// 待处理文件名
	src := g.Spider.Src
	filePath := filepath.Join(src, "config_spider", "spiders", "spider.py")

	// 替换 start_stage
	if err := utils.SetFileVariable(filePath, constants.AnchorStartStage, "parse_"+GetStartStageName(g.ConfigData)); err != nil {
		return err
	}

	// 替换 start_url
	if err := utils.SetFileVariable(filePath, constants.AnchorStartUrl, g.ConfigData.StartUrl); err != nil {
		return err
	}

	// 替换 parsers
	strParser := ""
	for _, stage := range g.ConfigData.Stages {
		stageName := stage.Name
		stageStr := g.GetParserString(stageName, stage)
		strParser += stageStr
	}
	if err := utils.SetFileVariable(filePath, constants.AnchorParsers, strParser); err != nil {
		return err
	}

	return nil
}

func (g ScrapyGenerator) GetParserString(stageName string, stage entity.Stage) string {
	// 构造函数定义行
	strDef := g.PadCode(fmt.Sprintf("def parse_%s(self, response):", stageName), 1)

	strParse := ""
	if stage.IsList {
		// 列表逻辑
		if g.ConfigData.Goose {
			// 使用goose 模块,过滤重复数据
			strParse = g.GetUniqueUrlListParserString(stageName, stage)
		} else {
			// 使用自定义模块
			strParse = g.GetListParserString(stageName, stage)
		}
	} else {
		// 非列表逻辑

		if g.ConfigData.Goose {
			// 使用goose 模块
			strParse = g.GetNonListGooesParserString(stageName, stage)
		} else {
			// 使用自定义模块
			strParse = g.GetNonListParserString(stageName, stage)
		}

	}

	// 构造
	str := fmt.Sprintf(`%s%s`, strDef, strParse)

	return str
}

func (g ScrapyGenerator) PadCode(str string, num int) string {
	res := ""
	for i := 0; i < num; i++ {
		res += "    "
	}
	res += str
	res += "\n"
	return res
}

func (g ScrapyGenerator) GetNonListParserString(stageName string, stage entity.Stage) string {
	str := ""

	// 获取或构造item
	str += g.PadCode("item = Item() if response.meta.get('item') is None else response.meta.get('item')", 2)

	// 遍历字段列表
	for _, f := range stage.Fields {
		line := fmt.Sprintf(`item['%s'] = response.%s.extract_first()`, f.Name, g.GetExtractStringFromField(f))
		line = g.PadCode(line, 2)
		str += line
	}

	// next stage 字段
	if f, err := g.GetNextStageField(stage); err == nil {
		// 如果找到 next stage 字段，进行下一个回调
		str += g.PadCode(fmt.Sprintf(`yield scrapy.Request(url="get_real_url(response, item['%s'])", callback=self.parse_%s, meta={'item': item})`, f.Name, f.NextStage), 2)
	} else {
		// 如果没找到 next stage 字段，返回 item
		str += g.PadCode(fmt.Sprintf(`yield item`), 2)
	}

	// 加入末尾换行
	str += g.PadCode("", 0)

	return str
}

func (g ScrapyGenerator) GetNonListGooesParserString(stageName string, stage entity.Stage) string {
	str := ""

	// 获取或构造item
	str += g.PadCode("item = Item() if response.meta.get('item') is None else response.meta.get('item')", 2)

	// 遍历字段列表
	str += g.PadCode(fmt.Sprintf(`article = goose.extract(raw_html=response.text)`), 2)
	str += g.PadCode(fmt.Sprintf(`item['title'] = article.title`), 2)
	str += g.PadCode(fmt.Sprintf(`item["content"]= article.cleaned_text`), 2)
	str += g.PadCode(fmt.Sprintf(`item['raw_html'] = article.raw_html`), 2)
	str += g.PadCode(fmt.Sprintf(`item['publish_datetime_utc'] = article.publish_datetime_utc`), 2)
	str += g.PadCode(fmt.Sprintf(`item['tags'] = article.tags`), 2)
	str += g.PadCode(fmt.Sprintf(`item['publish_date'] = article.publish_date`), 2)

	// next stage 字段
	if f, err := g.GetNextStageField(stage); err == nil {
		// 如果找到 next stage 字段，进行下一个回调
		str += g.PadCode(fmt.Sprintf(`yield scrapy.Request(url="get_real_url(response, item['%s'])", callback=self.parse_%s, meta={'item': item})`, f.Name, f.NextStage), 2)
	} else {
		// 如果没找到 next stage 字段，返回 item
		str += g.PadCode(fmt.Sprintf(`yield item`), 2)
	}

	// 加入末尾换行
	str += g.PadCode("", 0)

	return str
}

func (g ScrapyGenerator) GetListParserString(stageName string, stage entity.Stage) string {
	str := ""

	// 获取前一个 stage 的 item
	str += g.PadCode(`prev_item = response.meta.get('item')`, 2)

	// for 循环遍历列表
	str += g.PadCode(fmt.Sprintf(`for elem in response.%s:`, g.GetListString(stage)), 2)

	// 构造item
	str += g.PadCode(`item = Item()`, 3)

	// 遍历字段列表
	for _, f := range stage.Fields {
		line := fmt.Sprintf(`item['%s'] = elem.%s.extract_first()`, f.Name, g.GetExtractStringFromField(f))
		line = g.PadCode(line, 3)
		str += line
	}

	// 把前一个 stage 的 item 值赋给当前 item
	str += g.PadCode(`if prev_item is not None:`, 3)
	str += g.PadCode(`for key, value in prev_item.items():`, 4)
	str += g.PadCode(`item[key] = value`, 5)

	// next stage 字段
	if f, err := g.GetNextStageField(stage); err == nil {
		// 如果找到 next stage 字段，进行下一个回调
		str += g.PadCode(fmt.Sprintf(`yield scrapy.Request(url=get_real_url(response, item['%s']), callback=self.parse_%s, meta={'item': item})`, f.Name, f.NextStage), 3)
	} else {
		// 如果没找到 next stage 字段，返回 item
		str += g.PadCode(fmt.Sprintf(`yield item`), 3)
	}

	// 分页
	if stage.PageCss != "" || stage.PageXpath != "" {
		str += g.PadCode(fmt.Sprintf(`next_url = response.%s.extract_first()`, g.GetExtractStringFromStage(stage)), 2)
		str += g.PadCode(fmt.Sprintf(`yield scrapy.Request(url=get_real_url(response, next_url), callback=self.parse_%s, meta={'item': prev_item})`, stageName), 2)
	}

	// 加入末尾换行
	str += g.PadCode("", 0)

	return str
}

func (g ScrapyGenerator) GetUniqueUrlListParserString(stageName string, stage entity.Stage) string {
	str := ""

	// 获取前一个 stage 的 item
	str += g.PadCode(`prev_item = response.meta.get('item')`, 2)

	// for 循环遍历列表
	str += g.PadCode(fmt.Sprintf(`for elem in response.%s:`, g.GetListString(stage)), 2)

	// 构造item
	for _, f := range stage.Fields {
		if f.Name == "url" {
			str += g.PadCode(fmt.Sprintf(`url = elem.%s.extract_first()`, g.GetExtractStringFromField(f)), 3)
			str += g.PadCode("if unique_url(url):", 3)
			str += g.PadCode("continue", 4)
		}
	}
	str += g.PadCode(`item = Item()`, 3)

	// 遍历字段列表
	for _, f := range stage.Fields {
		line := fmt.Sprintf(`item['%s'] = elem.%s.extract_first()`, f.Name, g.GetExtractStringFromField(f))
		line = g.PadCode(line, 3)
		str += line
	}

	// 把前一个 stage 的 item 值赋给当前 item
	str += g.PadCode(`if prev_item is not None:`, 3)
	str += g.PadCode(`for key, value in prev_item.items():`, 4)
	str += g.PadCode(`item[key] = value`, 5)

	// next stage 字段
	if f, err := g.GetNextStageField(stage); err == nil {
		// 如果找到 next stage 字段，进行下一个回调
		str += g.PadCode(fmt.Sprintf(`yield scrapy.Request(url=get_real_url(response, item['%s']), callback=self.parse_%s, meta={'item': item})`, f.Name, f.NextStage), 3)
	} else {
		// 如果没找到 next stage 字段，返回 item
		str += g.PadCode(fmt.Sprintf(`yield item`), 3)
	}

	// 分页
	if stage.PageCss != "" || stage.PageXpath != "" {
		str += g.PadCode(fmt.Sprintf(`next_url = response.%s.extract_first()`, g.GetExtractStringFromStage(stage)), 2)
		str += g.PadCode(fmt.Sprintf(`yield scrapy.Request(url=get_real_url(response, next_url), callback=self.parse_%s, meta={'item': prev_item})`, stageName), 2)
	}

	// 加入末尾换行
	str += g.PadCode("", 0)

	return str
}

// 获取所有字段
func (g ScrapyGenerator) GetAllFields() []entity.Field {
	return GetAllFields(g.ConfigData)
}

// 获取包含 next stage 的字段
func (g ScrapyGenerator) GetNextStageField(stage entity.Stage) (entity.Field, error) {
	for _, field := range stage.Fields {
		if field.NextStage != "" {
			return field, nil
		}
	}
	return entity.Field{}, errors.New("cannot find next stage field")
}

func (g ScrapyGenerator) GetExtractStringFromField(f entity.Field) string {
	if f.Css != "" {
		// 如果为CSS
		if f.Attr == "" {
			// 文本
			return fmt.Sprintf(`css('%s::text')`, f.Css)
		} else {
			// 属性
			return fmt.Sprintf(`css('%s::attr("%s")')`, f.Css, f.Attr)
		}
	} else {
		// 如果为XPath
		if f.Attr == "" {
			// 文本
			return fmt.Sprintf(`xpath('string(%s)')`, f.Xpath)
		} else {
			// 属性
			return fmt.Sprintf(`xpath('%s/@%s')`, f.Xpath, f.Attr)
		}
	}
}

func (g ScrapyGenerator) GetExtractStringFromStage(stage entity.Stage) string {
	// 分页元素属性，默认为 href
	pageAttr := "href"
	if stage.PageAttr != "" {
		pageAttr = stage.PageAttr
	}

	if stage.PageCss != "" {
		// 如果为CSS
		return fmt.Sprintf(`css('%s::attr("%s")')`, stage.PageCss, pageAttr)
	} else {
		// 如果为XPath
		return fmt.Sprintf(`xpath('%s/@%s')`, stage.PageXpath, pageAttr)
	}
}

func (g ScrapyGenerator) GetListString(stage entity.Stage) string {
	if stage.ListCss != "" {
		return fmt.Sprintf(`css('%s')`, stage.ListCss)
	} else {
		return fmt.Sprintf(`xpath('%s')`, stage.ListXpath)
	}
}
