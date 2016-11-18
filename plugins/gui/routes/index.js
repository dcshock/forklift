var ensureAuthenticated = require('../utils/auth').ensureAuthenticated;
var passport = require('passport');
var dashboardController = require('../controllers/dashboardController.js');

module.exports = function(express) {
    var router = express.Router();
    router.get('/forklift-gui/dashboard/', ensureAuthenticated,  dashboardController.show);
    router.get('/forklift-gui/about/', ensureAuthenticated, dashboardController.showAbout);
    router.get('/forklift-gui/sendDailySummary/', dashboardController.sendDailySummary);

    // LOGIN //
    router.get('/', function(req, res) {
        res.redirect('/forklift-gui/');
    });
    router.get('/forklift-gui/', function (req, res, next) {
        res.redirect('/forklift-gui/login/');
    });
    router.get('/forklift-gui/health', function (req, res) {
        //If the app is running fine, then a health check should return 200.
        res.sendStatus(200);
    });
    router.get('/forklift-gui/login', function (req, res) {
        if (req.isAuthenticated()) {
            res.redirect('/forklift-gui/dashboard');
        } else {
            res.render('login');
        }
    });
    router.get('/forklift-gui/logout', function (req, res) {
        req.logout();
        res.redirect('/forklift-gui/login/');
    });
    router.get('/forklift-gui/auth/google', passport.authenticate('google', {scope: [
            "https://www.googleapis.com/auth/userinfo.profile",
            "https://www.googleapis.com/auth/userinfo.email"
        ]})
    );
    router.get('/forklift-gui/auth/google/callback',
        passport.authenticate('google', {
            successRedirect: process.env.GOOGLE_DOMAIN + 'dashboard',
            failureRedirect: process.env.GOOGLE_DOMAIN + 'login'
        })
    );

    return router;
};
