var o = {};
o.ensureAuthenticated = (req, res, next) => {
  if (req.isAuthenticated()) {
    return next();
  }

  res.status(401);
  res.render('401');
  return res.statusCode;
}

module.exports = o;