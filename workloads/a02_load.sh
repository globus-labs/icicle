for i in {1..10}; do
    ./evaluate_performance.sh 10000 0 0 &
    # ./evaluate_output.sh 10000 0 0 &
done
wait   # <- wait for all background jobs
echo "All evaluate_output.sh/evaluate_performance.sh jobs finished."
