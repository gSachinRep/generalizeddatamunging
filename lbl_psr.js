var fs = require("fs");
var args = process.argv.slice(2);
var LineByLineReader = require('line-by-line');

var lr = new LineByLineReader(args[0]);
var colHeaders = false;
var metadata = [];
var c_criteria = "State Code,Illiterate - Males,Illiterate - Females,Literate - Males,Literate - Females";
var f_criteria = "Age-group==All ages,Total/ Rural/ Urban==Total";
//var f_criteria = "Age-group==All ages,Total/ Rural/ Urban==Total,State Code==12,State Code==13,State Code==14,State Code==15,State Code==16,State Code==17,State Code==18";
var a_criteria = "Illiterate - Males,Illiterate - Females,Literate - Males,Literate - Females";
//var c_criteria = "Country Code,2005";
//var f_criteria = "Indicator Code==NY.GDP.MKTP.KD"
//,Indicator Code==NY.GDP.PCAP.KD,Indicator Code==NY.GNP.MKTP.KD,Indicator Code==NY.GNP.PCAP.KD
//var a_criteria = "2005";
var body = [];
var colIndex = [];
var finaldata = [];
var w_clauses = {};
var aggregate_criteria=[];
var opr = /(==|=|>=|<=|>|<|!=|<>|\sin\s)/;


lr.on('error', function (err) {
  if(err){
    console.log("Invalid file name or error reading the file!!");
  }
	// 'err' contains error object
});

lr.on('line', function (line) {
	// 'line' contains the current line without the trailing newline character.
  lr.pause();

  var select_criteria = arguments_to_array(c_criteria);
  if(colHeaders==false){
    metadata = generateMetadata(line);
    colHeaders = true;

    if(!select_criteria){
      select_criteria = metadata["columns"];
    }

    if(f_criteria!==""){
      var filter_criteria = arguments_to_array(f_criteria);
      w_clauses = buildWhereClauses(filter_criteria);
      var colforFilter =[];
      for(i=0;i<w_clauses.length;i++)
        colforFilter.push(w_clauses[i]["l_operand"]);
      buildColIndex(addColumntoSelect(colforFilter,select_criteria));
    }

    if(a_criteria!==""){
      aggregate_criteria = arguments_to_array(a_criteria);
      buildColIndex(addColumntoSelect(aggregate_criteria,select_criteria));
      //console.log(aggregate_criteria);
    }

  } else {
      var rowrecd = selectdatafilter(line,colIndex,w_clauses);
      if(rowrecd!==undefined){
        //console.log(colIndex);
        body.push(rowrecd);
      }
      if(aggregate_criteria!==undefined){
        finaldata = aggregate_data_by_col(body,aggregate_criteria)
      }
  }

  lr.resume();
});

lr.on('end', function () {
  //console.log(body);
  console.log(finaldata);
  metadata["data"] = JSON.stringify(body);
  var wf = fs.writeFileSync("myfile.json", JSON.stringify(body));
});

var addColumntoSelect = function(x,y){
  Array.prototype.unique = function() {
      var a = this.concat();
      for(var i=0; i<a.length; ++i+1) {
          for(var j=i+1; j<a.length; ++j) {
              if(a[i] === a[j])
                  a.splice(j--, 1);
          }
      }
      return a;
  };
  return y.concat(x).unique();
}
var buildWhereClauses = function(filter_criteria){
  var w_expressions = [];
  var i=0;
  filter_criteria.forEach(function(where_expression){
    var str = {};
    var operator = opr.exec(where_expression);
    var expression = where_expression.split(operator[0]);

    str["l_operand"] = expression[0];
    str["operator"] = operator[0];
    str["r_operand"] = expression[1];
    w_expressions[i] = str;
    i++;
  });
  return w_expressions;
}
var buildColIndex = function(select_criteria){
  for(var x=0;x < select_criteria.length;x++){
      var y = metadata["columns"].indexOf(select_criteria[x]);
      if(y>=0){colIndex.push(y)};
    }
}
var arguments_to_array = function(param){
  if(param != '') {
  if(param.indexOf(',') < 0) {
    var arr = [];
    arr.push(param);
    return arr;
  }
  else
    return param.split(',');
  }
}
var generateMetadata = function(line) {
    cols = [];
    m_data = {};
    cols = line.split(/,(?=(?:(?:[^"]*"){2})*[^"]*$)/);
    console.log(cols);
    m_data["filename"] = args[0];
    m_data["outputfile"] = args[1];
    m_data["columns"] = cols;
    m_data["totcols"] = cols.length;
    return m_data;
}
var selectdatafilter = function(linedata, colIndex, w_clauses){
  var filter = [];
  var row = {};
  var datarow = linedata.split(/,(?=(?:(?:[^"]*"){2})*[^"]*$)/);
  colIndex.forEach(function(colI){
    row[metadata["columns"][colI]] = datarow[colI];
  });
  console.log(w_clauses);
  if(w_clauses!="undefined"){
    w_clauses.forEach(function(i){
        if(row != null)
          //var cprevfield = i["l_operand"];

          row = selectdata(row, colIndex, i["l_operand"], i["r_operand"], i["operator"]);
    });
    if(row != null)
    return row;
  } else {
    return row;
  }
}
var selectdata = function(row, colIndex, columnName, columnValue, operator){
  var exp;
  colIndex.forEach(function(colI){
    if(metadata["columns"][colI] == columnName){
      if((typeof columnValue)=="number"){
        exp = "'"+ row[metadata["columns"][colI]] + "'" + operator + columnValue;
      } else {
        exp = "'"+ row[metadata["columns"][colI]] + "'" + operator + "'"+columnValue+"'";
      }
    }
  });
  if(eval(exp)){
    return row;
  } else {
    return null;
  }
}

function aggregate_data_by_col(data, aggregate_col){
  var filter = data;
  var datacol = {}
  datacol = aggregate_a_column(data, aggregate_col)
  filter = merge(datacol,data);
  return filter;
}

function merge(o, p) {
  for(prop in p) { // For all props in p.
  if (o.hasOwnProperty[prop]) continue; // Except those already in o.
    o[prop] = p[prop]; // Add the property to o.
  }
  return o;
}

function aggregate_a_column(data, aggregate){
  var tot = {};
  aggregate.forEach(function(col){
      var agg=0;
      for(i=0;i<data.length;i++){
          agg += isNaN(parseInt(data[i][col].toString())) ? 0 : parseInt(data[i][col].toString());
      }
      tot[col]=agg;
  });
  return tot;
}
