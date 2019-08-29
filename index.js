var log = require('logger')('model:index');
var async = require('async');
var _ = require('lodash');
var mongoose = require('mongoose');
var ObjectId = mongoose.Types.ObjectId;

var errors = require('errors');
var utils = require('utils');
var validators = require('validators');

var modelUpdatesQueueUrl;

var validate = function (name, ctx, done) {
  if (ctx.validated) {
    return done();
  }
  var validator = validators.model[name];
  validator.call(validator.model, ctx, done);
};

var diff = function (found, updated) {
  return utils.diff(utils.json(found), utils.json(updated));
};

var findModelUpdatesQueueUrl = function (done) {
  if (modelUpdatesQueueUrl) {
    return done(null, modelUpdatesQueueUrl);
  }
  utils.sqs().getQueueUrl({
    QueueName: utils.queue('model-updates') + '.fifo'
  }, function (err, o) {
    if (err) {
      return done(err);
    }
    modelUpdatesQueueUrl = o.QueueUrl;
    done(null, modelUpdatesQueueUrl);
  });
};

var updated = function (ctx, id, action, changes, done) {
  var data = {
    id: id,
    action: action,
    updated: changes,
    model: ctx.model.modelName
  };
  findModelUpdatesQueueUrl(function (err, queueUrl) {
    if (err) {
      return done(err);
    }
    var mid = utils.uuid();
    utils.sqs().sendMessage({
      MessageBody: utils.stringify(data),
      QueueUrl: queueUrl,
      MessageGroupId: mid,
      MessageDeduplicationId: mid
    }, done);
  });
};

exports.objectId = function (id) {
  return id.match(/^[0-9a-fA-F]{24}$/);
};

exports.ensureIndexes = function (schema, compounds) {
  var paths = schema.paths;
  Object.keys(paths).forEach(function (path) {
    var o = paths[path];
    var options = o.options || {};
    if (!options.searchable && !options.sortable) {
      return;
    }
    var index = {};
    index[path] = 1;
    if (!options.sortable) {
      schema.index(index);
      return;
    }
    index._id = 1;
    compounds.push(index);
    schema.index(index);
  });
  var extended = [];
  compounds.forEach(function (o) {
    schema.index(o);
    var exd = _.cloneDeep(o);
    exd[Object.keys(exd)[0]] = -1;
    schema.index(exd);
    extended.push(exd);
  });
  schema.compounds = compounds.concat(extended);
};

exports.cast = function (model, data) {
  var schema = model.schema;
  var paths = schema.paths;
  var field;
  var options;
  var type;
  for (field in data) {
    if (!data.hasOwnProperty(field)) {
      continue;
    }
    options = paths[field].options;
    type = options.type;
    if (field === '_id') {
      data[field] = new ObjectId(data[field]);
      continue
    }
    data[field] = new type(data[field]);
  }
  return data;
};

exports.invert = function (o) {
  var key;
  var clone = _.cloneDeep(o);
  for (key in clone) {
    if (!clone.hasOwnProperty(key)) {
      continue;
    }
    clone[key] *= -1;
  }
  return clone;
};

exports.first = function (o) {
  var key;
  for (key in o) {
    if (!o.hasOwnProperty(key)) {
      continue;
    }
    return key;
  }
  return null;
};

exports.cursor = function (index, o) {
  var field;
  var cursor = {};
  for (field in index) {
    if (!index.hasOwnProperty(field)) {
      continue;
    }
    cursor[field] = o[field];
  }
  return cursor;
};

exports.create = function (ctx, done) {
  validate('create', ctx, function (err) {
    if (err) {
      return done(err);
    }
    ctx.model.create(ctx.data, function (err, o) {
      if (err) {
        return done(err);
      }
      updated(ctx, o.id, 'create', diff({}, o), function (err) {
        done(err, o);
      });
    });
  });
};

exports.update = function (ctx, done) {
  validate('update', ctx, function (err) {
    if (err) {
      return done(err);
    }
    ctx.model.findOneAndUpdate(ctx.query, ctx.data, {new: true}, function (err, o) {
      if (err) {
        return done(err);
      }
      if (!o) {
        return done(errors.notFound());
      }
      updated(ctx, o.id, 'update', diff(ctx.found, o), function (err) {
        done(err, o);
      });
    });
  });
};

exports.findOne = function (ctx, done) {
  validate('findOne', ctx, function (err) {
    if (err) {
      return done(err);
    }
    ctx.model.findOne(ctx.query).exec(function (err, o) {
      if (err) {
        return done(err);
      }
      if (!o) {
        return done(errors.notFound());
      }
      utils.visibles(ctx, utils.json(o), done);
    });
  });
};

exports.remove = function (ctx, done) {
  validate('remove', ctx, function (err) {
    if (err) {
      return done(err);
    }
    ctx.model.remove(ctx.query).exec(function (err, o) {
      if (err) {
        return done(err);
      }
      if (!o.n) {
        return done(errors.notFound());
      }
      done(null, o);
    });
  });
};

// TODO cursor without direction is an invalid query
exports.find = function (ctx, done) {
  validate('find', ctx, function (err) {
    if (err) {
      return done(err);
    }
    var hint;
    var invert;
    var sorter;
    var search = ctx.search;
    var query = search.query;
    var sort = search.sort;
    var count = search.count + 1;
    var order = sort[exports.first(sort)];
    var direction = search.direction || order;
    var natural = (direction === 1);
    if (order === 1) {
      hint = sort;
      invert = !natural;
      sorter = invert ? exports.invert(sort) : sort;
    } else {
      hint = exports.invert(sort);
      invert = natural;
      sorter = invert ? hint : sort;
    }
    var fields = _.clone(search.fields);
    if (fields) {
      fields.visibility = 1;
    }
    var options = {};
    if (search.cursor) {
      if (natural) {
        options.min = search.cursor;
      } else {
        options.max = search.cursor;
      }
    }
    var filter = function (o) {
      if (!fields) {
        return o;
      }
      if (fields.visibility) {
        return o;
      }
      var filtered = {};
      Object.keys(o).forEach(function (field) {
        if (fields[field]) {
          filtered[field] = o[field];
        }
      });
      return filtered;
    };
    // TODO: build proper cursor with fields in order
    var queried = ctx.queried;
    ctx.model.find(query)
      .sort(sorter)
      .select(fields)
      .limit(count)
      .hint(hint)
      .setOptions(options)
      .exec(function (err, oo) {
        if (err) {
          return done(err);
        }
        var left = null;
        var right = null;
        if (natural) {
          if (oo.length === count) {
            right = {
              query: queried,
              sort: sort,
              cursor: exports.cursor(hint, oo.pop()),
              direction: 1
            };
          }
          if (search.cursor) {
            left = {
              query: queried,
              sort: sort,
              cursor: search.cursor,
              direction: -1
            };
          }
        } else {
          if (search.cursor) {
            right = {
              query: queried,
              sort: sort,
              cursor: search.cursor,
              direction: 1
            };
          }
          if (oo.length === count) {
            oo.pop();
            left = {
              query: queried,
              sort: sort,
              cursor: exports.cursor(hint, oo[oo.length - 1]),
              direction: -1
            };
          }
        }
        var last;
        var next;
        if (order === 1) {
          next = right;
          last = left;
        } else {
          next = left;
          last = right;
        }
        oo = invert ? oo.reverse() : oo;
        var ooo = [];
        async.eachSeries(oo, function (o, eachDone) {
          utils.visibles(ctx, filter(utils.json(o)), function (err, o) {
            if (err) {
              return eachDone(err);
            }
            ooo.push(o);
            eachDone();
          })
        }, function (err) {
          if (err) {
            return done(err);
          }
          done(null, ooo, {last: last, next: next})
        });
      });
  });
};
