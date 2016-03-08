// Express packages
var express = require('express');
var favicon = require('serve-favicon');
var path = require('path');
var bodyParser = require('body-parser');

// Auth packages
var passport = require('passport');
var cookieSession = require('cookie-session');
var GoogleStrategy = require('passport-google-oauth2').Strategy;

var app = express();

// Static assets
app.use('/img', express.static(path.join(__dirname, 'public/images')));
app.use('/js', express.static(path.join(__dirname, 'public/javascripts')));
app.use('/css', express.static(path.join(__dirname, 'public/stylesheets')));
app.use('/fonts', express.static(path.join(__dirname, 'public/fonts')));
app.use(favicon(path.join(__dirname, '/public/images/', 'favicon.png')));

// Logging stack
var logger = require('./utils/logger');
logger.info("Forklift-Gui is running");
app.use(logger.errorMorgan);

// view engine setup
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'jade');
app.set('trust proxy', 1) // trust first proxy

// Authentication
app.use(cookieSession({
  secret: process.env.SESSION_SECRET || "this should really be changed"
}));
app.use(passport.initialize());
app.use(passport.session());
passport.serializeUser((user, done) => done(null, user.email));
passport.deserializeUser((email, done) => done(null, email));
passport.use(new GoogleStrategy({
        clientID: process.env.GOOGLE_KEY,
        clientSecret: process.env.GOOGLE_SECRET,
        callbackURL: process.env.GOOGLE_DOMAIN + "auth/google/callback"
    },
    (accessToken, refreshToken, profile, done) => {
        // asynchronous verification
        process.nextTick(() => done(null, profile));
    }
));

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({extended: false}));

// Wire route handlers
app.use(require('./routes'));

// any non routed request is given a 404
app.get('*', (req, res) => {
    res.status(404);
    res.render('404');
});

module.exports = app;