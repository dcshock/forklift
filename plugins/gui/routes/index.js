var express = require('express');
var passport = require('passport');
var logger = require('../utils/logger');
var config = require('../config/config');
var router = express.Router();

router.use('/dashboard', require('./elasticSearch'));
router.use('/dashboard', require('./retry'));

router.get('/', function (req, res, next) {
    res.redirect('login');
});

router.get('/health', function (req, res) {
    //If the app is running fine, then a health check should return 200.
    res.sendStatus(200);
});

router.get('/forklift-gui/health', function (req, res) {
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
    if (sofiEmail[1] == 'sofi.org' || sofiEmail[1] == 'sofi.com') {
        GLOBAL.user = sofiEmail[0];
        res.render('dashboard');
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
