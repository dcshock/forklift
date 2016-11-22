var elasticController = require('../controllers/elasticSearchController.js');
var ensureAuthenticated = require('../utils/auth').ensureAuthenticated;

module.exports = function(express) {
    var router = express.Router();
    router.post('/fixed', ensureAuthenticated, elasticController.updateAsFixed);
    router.post('/fixAll', ensureAuthenticated, elasticController.updateAllAsFixed);
    router.post('/retry', ensureAuthenticated, elasticController.retry);
    router.get('/retries', ensureAuthenticated, elasticController.showRetries);
    router.get('/replays', ensureAuthenticated, elasticController.showReplays);
    router.get('/filtered', ensureAuthenticated, elasticController.showFilteredResults);

    return router;
};
