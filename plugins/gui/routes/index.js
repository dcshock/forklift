var express = require('express');
var passport = require('passport');
var logger = require('../utils/logger');
var config = require('../config/config');
var router = express.Router();

router.use('/dashboard', require('./elasticSearch'))

router.get('/', function (req, res, next) {
    res.redirect('login')
});

router.get('/health', function (req, res) {
    //If the app is running fine, then a health check should return 200.
    res.sendStatus(200);
});

router.get('/noise/health', function (req, res) {
    res.sendStatus(200);
});

router.get('/login', function (req, res) {
    if (req.isAuthenticated()) {
        res.redirect('dashboard');
    } else {
        res.render('login');
    }
});

router.get('/logout', function (req, res) {
    req.logout();
    res.redirect('login');
});

router.get('/dashboard', ensureAuthenticated,  function (req, res) {
    //Get the users email, name, and profile picture
    var sofiEmail = req.user.emails[0].value.split("@");
    //var profilePicUrl = req.user.photos[0].value.split("?");
    //profilePicUrl = profilePicUrl[0] + "?sz=100";
    if (sofiEmail[1] == 'sofi.org') {
        GLOBAL.user = sofiEmail[0];
        res.render('dashboard');
        //res.render('dashboard', {user: req.user, email: req.user.emails[0].value});
    } else {
        req.logout();
        res.status(401);
        res.render('401');
    }
});

router.get('/about', ensureAuthenticated, function (req, res) {
    res.render('about');
});

router.get('/auth/google',
    passport.authenticate('google', {scope: config.google.scope}
    ));

router.get('/auth/google/callback',
    passport.authenticate('google', {
        successRedirect: config.google.domain + 'dashboard',
        failureRedirect: config.google.domain + 'login'
    })
);


function ensureAuthenticated(req, res, next) {
    if (req.isAuthenticated()) {
        return next();
    }
    logger.info("Unauthorized");
    res.status(401);
    res.render('401');
    return res.statusCode;
}

module.exports = router;
