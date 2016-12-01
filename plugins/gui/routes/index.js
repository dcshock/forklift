var ensureAuthenticated = require('../utils/auth').ensureAuthenticated;
var passport = require('passport');
var dashboardController = require('../controllers/dashboardController.js');

module.exports = function(express) {
    var router = express.Router();
    router.get('/dashboard', ensureAuthenticated,  dashboardController.show);
    router.get('/about', ensureAuthenticated, dashboardController.showAbout);
    router.get('/sendDailySummary', dashboardController.sendDailySummary);

    // LOGIN //
    router.get('/', function (req, res, next) {
        res.redirect('login');
    });
    router.get('/health', function (req, res) {
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

    return router;
};
