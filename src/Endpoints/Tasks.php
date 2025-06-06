<?php

declare(strict_types=1);

namespace Meilisearch\Endpoints;

use Meilisearch\Contracts\CancelTasksQuery;
use Meilisearch\Contracts\DeleteTasksQuery;
use Meilisearch\Contracts\Endpoint;
use Meilisearch\Exceptions\TimeOutException;

class Tasks extends Endpoint
{
    protected const PATH = '/tasks';

    public function get($taskUid): array
    {
        return $this->http->get(self::PATH.'/'.$taskUid);
    }

    public function all(array $query = []): array
    {
        return $this->http->get(self::PATH.'/', $query);
    }

    public function cancelTasks(?CancelTasksQuery $options): array
    {
        $options = $options ?? new CancelTasksQuery();

        return $this->http->post('/tasks/cancel', null, $options->toArray());
    }

    public function deleteTasks(?DeleteTasksQuery $options): array
    {
        $options = $options ?? new DeleteTasksQuery();

        return $this->http->delete(self::PATH, $options->toArray());
    }

    /**
     * @throws TimeOutException
     */
    public function waitTask($taskUid, int $timeoutInMs, int $intervalInMs): array
    {
        $intervalInMs = $intervalInMs < 500 ? 500 : $intervalInMs;

        $this->log(date("c") . ' waitTask called with taskUid: ' . $taskUid . ', timeoutInMs: ' . $timeoutInMs . ', intervalInMs: ' . $intervalInMs);

        $timeoutTemp = 0;

        while ($timeoutInMs > $timeoutTemp) {
            try {
                $res = $this->get($taskUid);

                if ('enqueued' !== $res['status'] && 'processing' !== $res['status']) {
                    $this->log(date("c") . " Task {$taskUid} completed with status: {$res['status']} after {$timeoutTemp} ms");

                    return $res;
                }
            } catch (\Throwable $ce) {
                if(str_starts_with($ce->getMessage(), "Idle timeout reached for")) {
                    $this->log(date("c") . " Task {$taskUid} after {$timeoutTemp} ms ran into IdleTimeoutReached... retrying.");
                } else {
                    throw $ce;
                }
            }

            $timeoutTemp += $intervalInMs;
            usleep(1000 * $intervalInMs);

            $this->log(date("c") . " Iteration done, current timeout: {$timeoutTemp} of {$timeoutInMs} ms");
        }

        throw new TimeOutException("Task {$taskUid} did not complete within the timeout of {$timeoutInMs} ms, waited {$timeoutTemp} ms - last known status: " . ($res['status'] ?? 'unknown') . ".");
    }

    private function log(string $message): void
    {
        if(method_exists("dump")) {
            dump($message);
        } else {
            var_dump($message);
        }
    }

    /**
     * @throws TimeOutException
     */
    public function waitTasks(array $taskUids, int $timeoutInMs, int $intervalInMs): array
    {
        $tasks = [];

        foreach ($taskUids as $taskUid) {
            $tasks[] = $this->waitTask($taskUid, $timeoutInMs, $intervalInMs);
        }

        return $tasks;
    }
}
