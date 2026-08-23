import express from 'express';
import { register, login } from '../contorller/authController.js';
import { getProjects, createQueue, togglePauseQueue, getQueues } from '../contorller/queueController.js';
import { createJob, getJobs, createBatchJobs, createRecurringJob } from '../contorller/jobController.js';
import {authenticateToken} from '../middleware/auth.js';

const router = express.Router();

// Public Auth Endpoints
router.post('/auth/register', register);
router.post('/auth/login', login);

// Protected Endpoints
router.use(authenticateToken);

// Projects & Queues
router.get('/projects', getProjects);
router.get('/queues', getQueues);
router.post('/queues', createQueue);
router.patch('/queues/:queueId/pause', togglePauseQueue);

// Jobs
router.get('/jobs', getJobs);
router.post('/jobs', createJob);
router.post('/jobs/batch', createBatchJobs);
router.post('/jobs/recurring', createRecurringJob);

export default router;