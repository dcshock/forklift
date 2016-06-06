var ensureAuthenticated = require('../utils/auth').ensureAuthenticated;
var express = require('express');
var logger = require('../utils/logger');
var passport = require('passport');
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
    var domain = req.user.split("@")[1];
    if (domain == 'sofi.org' || domain == 'sofi.com') {
        res.render('dashboard');
    } else {
        req.logout();
        res.status(401);
        res.render('401');
    }
});

router.get('/about', ensureAuthenticated, (req, res) => res.render('about'))
router.get('/auth/google', passport.authenticate('google', {scope: [
      "https://www.googleapis.com/auth/userinfo.profile",
      "https://www.googleapis.com/auth/userinfo.email"
    ]})
);
router.get('/auth/google/callback',
    passport.authenticate('google', {
        successRedirect: process.env.GOOGLE_DOMAIN + 'dashboard',
        failureRedirect: process.env.GOOGLE_DOMAIN + 'login'
    })
);

module.exports = router;
