var express = require('express');
var app = express();
var path = require('path');
var favicon = require('serve-favicon');
var cookieParser = require('cookie-parser');
var bodyParser = require('body-parser');
var config = require('./config/config.json');
var passport = require('passport');
var session = require('express-session');
var logger = require('./utils/logger');
var morgan = require('morgan');
var GoogleStrategy = require('passport-google-oauth2').Strategy;

logger.info("Forklift-Gui is running");

passport.serializeUser(function (user, done) {
    done(null, user);
});

passport.deserializeUser(function (obj, done) {
    done(null, obj);
});
passport.use(new GoogleStrategy({
        clientID: config.google.key,
        clientSecret: config.google.secret,
        callbackURL: config.google.domain + config.google.callback
    },
    function (accessToken, refreshToken, profile, done) {
        // asynchronous verification
        process.nextTick(function () {
            return done(null, profile);
        });
    }
));

app.use(morgan('combined', {
    skip: function (req, res) {
        return res.statusCode < 400
    },
    stream: logger.stream
}));

var app = express();

// view engine setup
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'jade');

app.use(favicon(path.join(__dirname, '/public/images/', 'favicon.png')));
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({extended: false}));
app.use(cookieParser());
app.use(session({
    secret: config.session.secret,
    saveUninitialized: true,
    resave: true
}));
app.use(passport.initialize());
app.use(passport.session());
app.use('/img', express.static(path.join(__dirname, 'public/images')));
app.use('/js', express.static(path.join(__dirname, 'public/javascripts')));
app.use('/css', express.static(path.join(__dirname, 'public/stylesheets')));
app.use('/fonts', express.static(path.join(__dirname, 'public/fonts')));
app.use(require('./routes'));

// any non routed request is given a 404
app.get('*', function (req, res) {
    res.status(404);
    res.render('404');
    return res.statusCode;
});


module.exports = app;

