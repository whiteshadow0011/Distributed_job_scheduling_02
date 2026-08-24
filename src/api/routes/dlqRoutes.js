import { Router } from 'express';
import { getDLQJobs, replayDLQJobs } from '../contorller/dlqController.js';
import { authenticateToken } from '../middleware/auth.js';

const router = Router({ mergeParams: true });

// All DLQ endpoints are protected
router.use(authenticateToken);

router.get('/', getDLQJobs);
router.post('/replay', replayDLQJobs);

export default router;